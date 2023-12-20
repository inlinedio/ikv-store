package io.inline.gateway;

import com.inlineio.schemas.Services;
import io.inline.gateway.ddb.IKVStoreContextObjectsAccessor;
import io.inline.gateway.ddb.beans.IKVStoreContext;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

public class UserStoreContextAccessor {
  private final IKVStoreContextObjectsAccessor _contextSot;
  private final ConcurrentHashMap<String, UserStoreContext> _contextCache;

  public UserStoreContextAccessor(IKVStoreContextObjectsAccessor ikvStoreContextObjectsAccessor) {
    _contextSot = Objects.requireNonNull(ikvStoreContextObjectsAccessor);
    _contextCache = new ConcurrentHashMap<>();
  }

  public Optional<UserStoreContext> getCtx(Services.UserStoreContextInitializer initializer)
      throws IllegalArgumentException {
    Objects.requireNonNull(initializer);

    // TODO: match credentials
    String accountId = initializer.getCredentials().getAccountId();
    String storeName = initializer.getStoreName();

    // Lookup from cache
    String key = String.join(",", accountId, storeName);
    @Nullable UserStoreContext ctx = _contextCache.get(key);
    if (ctx != null) {
      return Optional.of(ctx);
    }

    // Lookup from DynamoDB
    Optional<IKVStoreContext> maybeIKVStoreContext = _contextSot.getItem(accountId, storeName);
    if (maybeIKVStoreContext.isEmpty()) {
      return Optional.empty();
    }

    ctx = UserStoreContext.from(maybeIKVStoreContext.get());

    // Update Cache
    _contextCache.putIfAbsent(key, ctx);

    return Optional.of(ctx);
  }
}
