package io.inline.gateway;

import com.google.common.collect.ImmutableMap;
import com.inlineio.schemas.Common;
import com.inlineio.schemas.Services;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

// Cache over dynamodb
public class UserStoreContextFactory {
    private final static UserStoreContextFactory SINGLETON_INSTANCE = new UserStoreContextFactory();
    public static UserStoreContextFactory getInstance() {
        return SINGLETON_INSTANCE;
    }


    private final ConcurrentHashMap<Key, Value> _contexts;

    private UserStoreContextFactory() {
        _contexts = new ConcurrentHashMap<>();

        // TODO: remove these hardcoded entries
        String accountName = "testing-account";
        String storeName = "testing-store";
        String primaryKey = "userid";
        String partitioningKey = "userid";
        Map<String, Common.FieldSchema> schema = new ImmutableMap.Builder<String, Common.FieldSchema>()
                .put("userid", Common.FieldSchema.newBuilder()
                        .setName("userid")
                        .setId(0)
                        .setFieldType(Common.FieldType.STRING)
                        .build())
                .build();
        String kafkaTopic = "testing-topic";
        UserStoreContext context = new UserStoreContext(
                accountName, storeName, primaryKey, partitioningKey, schema, kafkaTopic);
        Key key = new Key(accountName, storeName);
        Value value = new Value(Common.AccountCredentials.newBuilder()
                .setAccountId(accountName)
                .setAccountPasskey("testing-passkey")
                .build(), context);
        _contexts.put(key, value);
    }

    public Optional<UserStoreContext> getCtx(Services.UserStoreContextInitializer initializer) throws IllegalArgumentException {
        Objects.requireNonNull(initializer);

        // Lookup from cache!
        // TODO: needs dynamodb integration
        Key key = new Key(initializer.getCredentials().getAccountId(), initializer.getStoreName());
        @Nullable Value value = _contexts.get(key);
        if (value == null) {
            return Optional.empty();
        }

        // match credentials
        String incomingAccountPasskey = initializer.getCredentials().getAccountPasskey();
        if (!value.accountCredentials().getAccountPasskey().equals(incomingAccountPasskey)) {
            // TODO: propagate correctly
            throw new IllegalArgumentException("Not Authorized");
        }

        return Optional.of(value.userStoreContext());
    }

    private static final class Key {
        private final String _accountId;
        private final String _storeName;

        public Key(String accountId, String storeName) {
            _accountId = accountId;
            _storeName = storeName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(_accountId, _storeName);
        }

        @Override
        public boolean equals(Object obj) {
            // TODO: proper impl
            return _accountId.equals(((Key) obj)._accountId)
                    && _storeName.equals(((Key) obj)._storeName);
        }
    }

    private static final class Value {
        private final Common.AccountCredentials _accountCredentials;
        private final UserStoreContext _userStoreContext;

        public Value(Common.AccountCredentials accountCredentials, UserStoreContext userStoreContext) {
            _accountCredentials = accountCredentials;
            _userStoreContext = userStoreContext;
        }

        public Common.AccountCredentials accountCredentials() {
            return _accountCredentials;
        }

        public UserStoreContext userStoreContext() {
            return _userStoreContext;
        }
    }
}
