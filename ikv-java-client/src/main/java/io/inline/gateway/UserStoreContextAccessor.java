package io.inline.gateway;

import com.google.protobuf.InvalidProtocolBufferException;
import com.inlineio.schemas.Common;
import com.inlineio.schemas.Services;
import io.inline.gateway.ddb.IKVStoreContextController;
import io.inline.gateway.ddb.beans.IKVStoreContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class UserStoreContextAccessor {
    private static final Logger LOGGER = LogManager.getLogger(UserStoreContextAccessor.class);

    private final IKVStoreContextController _contextSot;
    private final ConcurrentHashMap<String, UserStoreContext> _contextCache;

    public UserStoreContextAccessor(IKVStoreContextController ikvStoreContextController) {
        _contextSot = Objects.requireNonNull(ikvStoreContextController);
        _contextCache = new ConcurrentHashMap<>();
    }

    public Optional<UserStoreContext> getCtx(Services.UserStoreContextInitializer initializer) throws IllegalArgumentException {
        Objects.requireNonNull(initializer);
        String accountId = initializer.getCredentials().getAccountId();
        String storeName = initializer.getStoreName();

        // TODO: match credentials

        // Lookup from cache
        String key = String.join("", accountId, storeName);
        @Nullable UserStoreContext ctx = _contextCache.get(key);
        if (ctx != null) {
            return Optional.of(ctx);
        }

        // Lookup from DynamoDB
        Optional<IKVStoreContext> maybeIKVStoreContext = _contextSot.getItem(accountId, storeName);
        if (maybeIKVStoreContext.isEmpty()) {
            return Optional.empty();
        }

        // can throw proto deser exception, propagated as internal server error
        try {
            ctx = UserStoreContext.from(maybeIKVStoreContext.get());
        } catch (InvalidProtocolBufferException e) {
            LOGGER.error("Cannot parse IKVStoreContext protobuf: ", e);
            throw new IllegalStateException(e);
        }

        // Update Cache
        _contextCache.putIfAbsent(key, ctx);

        return Optional.of(ctx);
    }


    // @throws IllegalArgumentException if this field is already stored.
    public void registerSchemaForNewFields(Services.UserStoreContextInitializer initializer, Collection<Common.FieldSchema> fieldsToAdd) {
        Objects.requireNonNull(initializer);
        Objects.requireNonNull(fieldsToAdd);

        if (fieldsToAdd.size() == 0) {
            return;
        }

        // Check if this is a valid store and credentials match
        if (getCtx(initializer).isEmpty()) {
            throw new IllegalArgumentException(String.format("Not a valid store: %s", initializer.getStoreName()));
        }

        String accountId = initializer.getCredentials().getAccountId();
        String storeName = initializer.getStoreName();

        // write to dynamodb
        try {
            for (Common.FieldSchema field : fieldsToAdd) {
                _contextSot.registerSchemaForNewField(accountId, storeName, field);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // evict outdated copies from cache
            String key = String.join("", accountId, storeName);
            _contextCache.remove(key);
        }
    }
}
