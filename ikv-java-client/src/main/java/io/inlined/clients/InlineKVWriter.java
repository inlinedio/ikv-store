package io.inlined.clients;

import java.util.Collection;

public interface InlineKVWriter {
  /**
   * Blocking startup method. Ensure the writer startup has finished before invoking any other
   * upsert/delete/etc. operations.
   *
   * @throws RuntimeException for initialization errors
   */
  void startupWriter();

  /**
   * Shutdown the writer. No operations like upsert/delete should be onvoked after. The same writer
   * can be re-used by invoking {@link InlineKVWriter#startupWriter()}.
   */
  void shutdownWriter();

  /**
   * Upsert (insert or update) fields for a document.
   *
   * <p>Note: the partial document MUST contain primary key and the partitioning key (if different
   * than the primary key) for the document.
   *
   * <p>Updating the value of a primary key is equivalent to inserting a new document.
   *
   * <p>Updating the value of a partitioning key inserts the document to other IKVStore partitions,
   * while retaining it as is in the original partition associated with the partitioning key.
   *
   * @param partialDocument subset of fields that need to be upsert-ed. Remaining fields remain
   *     as-is.
   */
  void upsertFieldValues(IKVDocument partialDocument);

  // void batchUpsertFieldValues(Collection<IKVDocument> documents);

  /**
   * Delete specified fields from a document. Other fields remain as is. NO OP if the fields are not
   * present in the document.
   *
   * <p>It is allowed to delete the value of the primary and optional partitioning key from the
   * document body.
   *
   * @param documentId primary key and partitioning key (if different from primary key)
   * @param fieldsToDelete field names to remove from specified document-id
   */
  void deleteFieldValues(IKVDocument documentId, Collection<String> fieldsToDelete);

  // void batchDeleteFieldValues(Collection<IKVDocument> documentIds, Collection<String>
  // fieldsToDelete);

  /**
   * Delete a document if it exists. NO OP if document is not indexed.
   *
   * @param documentId primary key and partitioning key (if different from primary key)
   */
  void deleteDocument(IKVDocument documentId);

  // void batchDeleteDocuments(Collection<IKVDocument> documentIds);
}
