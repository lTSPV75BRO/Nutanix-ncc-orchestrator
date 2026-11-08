/** Save a Blob to disk via a transient anchor. */
export function triggerBlobDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  // Revoke after the click has been handled; some browsers cancel the
  // download when the URL is revoked synchronously.
  setTimeout(() => URL.revokeObjectURL(url), 0);
}
