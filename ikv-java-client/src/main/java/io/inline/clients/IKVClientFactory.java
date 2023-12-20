package io.inline.clients;

import java.util.Objects;

public class IKVClientFactory {
  private final ClientOptions _clientOptions;

  public IKVClientFactory(ClientOptions clientOptions) {
    _clientOptions = Objects.requireNonNull(clientOptions);
  }

  public InlineKVReader createNewReaderInstance() {
    return new DefaultInlineKVReader(_clientOptions);
  }

  public InlineKVWriter createNewWriterInstance() {
    return new DefaultInlineKVWriter(_clientOptions);
  }
}
