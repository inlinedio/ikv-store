package io.inlined.clients;

import com.google.common.annotations.VisibleForTesting;
import com.inlineio.schemas.Common;
import java.util.Objects;

public class IKVClientFactory {
  private final ClientOptions _clientOptions;

  public IKVClientFactory(ClientOptions clientOptions) {
    _clientOptions = Objects.requireNonNull(clientOptions);
  }

  public InlineKVReader createNewReaderInstance() {
    // TODO: remove server side config fetching
    ServerSuppliedConfigFetcher fetcher = new ServerSuppliedConfigFetcher(_clientOptions);
    Common.IKVStoreConfig serverConfig = fetcher.fetchServerConfig();
    Common.IKVStoreConfig clientSuppliedConfig = _clientOptions.asIKVStoreConfig();
    Common.IKVStoreConfig mergedConfig = mergeConfigs(clientSuppliedConfig, serverConfig);

    return new DefaultInlineKVReader(_clientOptions, mergedConfig);
  }

  @VisibleForTesting
  public static Common.IKVStoreConfig mergeConfigs(
      Common.IKVStoreConfig clientCfg, Common.IKVStoreConfig serverCfg) {
    return Common.IKVStoreConfig.newBuilder().mergeFrom(serverCfg).mergeFrom(clientCfg).build();
  }

  public InlineKVWriter createNewWriterInstance() {
    return new DefaultInlineKVWriter(_clientOptions);
  }
}
