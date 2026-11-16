"use strict";

const explorer = document.querySelector("#intake-explorer");
if (explorer) {
  const form = explorer.querySelector("[data-upload-form]");
  const picker = form.querySelector('input[type="file"]');
  const startButton = form.querySelector("[data-upload-start]");
  const chosenFolder = form.dataset.uploadFolder;
  const showMessage = text => {
    document.querySelectorAll("[data-intake-message]").forEach(message => { message.textContent = text; });
  };
  const results = explorer.querySelector("[data-upload-results]");
  const csrf = form.querySelector('[name="csrf"]').value;
  const limit = Number(explorer.dataset.maxUploadBytes);
  const items = [];
  let busy = false;
  let closed = false;

  const request = async (url, options) => {
    const response = await fetch(url, options);
    const body = await response.json();
    if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
    if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error("Invalid storage response");
    return body;
  };
  const post = (url, values = {}, options = {}) => {
    const body = new FormData();
    body.append("csrf", csrf);
    for (const [key, value] of Object.entries(values)) {
      for (const item of [value].flat()) body.append(key, String(item));
    }
    return request(url, { method: "POST", body, ...options });
  };
  const controls = () => {
    picker.disabled = busy;
    startButton.disabled = busy || !items.some(item => item.state === "pending");
    for (const item of items) item.retry.disabled = busy;
  };
  const withBusy = async task => {
    if (busy) { showMessage("Wait for the current transfer or confirmation."); return; }
    busy = true;
    controls();
    try { await task(); }
    catch (error) { showMessage(`Storage action failed: ${error.message}`); }
    finally { busy = false; controls(); }
  };
  const operationUrl = id => explorer.dataset.statusUrl.replace("OPERATION", encodeURIComponent(id));
  const poll = async (id, cancelled, update) => {
    while (!closed && !cancelled()) {
      const record = await request(operationUrl(id));
      if (typeof record.state !== "string") throw new Error("Invalid operation status");
      const state = record.state.toLowerCase();
      update(record);
      if (["succeeded", "completed"].includes(state)) return record;
      if (["failed", "cancelled", "recovery", "cleanup_pending"].includes(state)) throw new Error(record.message || state);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    throw new Error("Transfer cancelled; queued verification jobs remain in Jobs.");
  };

  const modal = explorer.querySelector("[data-intake-dialog]");
  const modalForm = modal.querySelector("form");
  const modalValue = modal.querySelector('[name="value"]');
  const modalCheck = modal.querySelector('[name="confirm"]');
  let resolveModal = null;
  let returnFocus = null;
  const ask = (title, detail, replace = false, allowRoot = false) => {
    modal.querySelector("#intake-dialog-title").textContent = title;
    modal.querySelector("[data-intake-dialog-detail]").textContent = detail;
    modal.querySelector("[data-intake-dialog-field]").hidden = replace;
    modal.querySelector("[data-intake-confirm-field]").hidden = !replace;
    modalValue.required = !replace && !allowRoot;
    modalValue.value = "";
    modalCheck.required = replace;
    modalCheck.checked = false;
    returnFocus = document.activeElement;
    modal.showModal();
    return new Promise(resolve => { resolveModal = resolve; });
  };
  modalForm.addEventListener("submit", event => {
    event.preventDefault();
    if (modalCheck.required && !modalCheck.checked) {
      modal.querySelector("[data-intake-dialog-detail]").textContent = "Check the replacement confirmation before continuing.";
      return;
    }
    if (modalValue.required && !modalValue.value.trim()) {
      modal.querySelector("[data-intake-dialog-detail]").textContent = "Enter a new name or folder path before continuing.";
      return;
    }
    const resolve = resolveModal;
    resolveModal = null;
    modal.close();
    resolve(modalCheck.required ? true : modalValue.value);
  });
  modal.querySelector("[data-intake-dialog-cancel]").addEventListener("click", () => modal.close());
  modal.addEventListener("close", () => {
    if (resolveModal) { resolveModal(null); resolveModal = null; }
    if (returnFocus) returnFocus.focus();
  });
  const replacement = async path => {
    if (closed) return null;
    const review = await post(explorer.dataset.reviewUrl, { path });
    if (closed) return null;
    if (!review.exists) return {};
    const approved = await ask("Replace existing video?", `${review.path}: ${review.size} bytes. This replaces the reviewed version and invalidates old recording overrides.`, true);
    if (!approved) return null;
    if (typeof review.confirmation !== "string") throw new Error("Invalid replacement confirmation");
    return { [review.path]: review.confirmation };
  };

  const status = (item, state, text) => {
    item.state = state;
    item.text.textContent = text;
    item.retry.hidden = !["failed", "cancelled"].includes(state);
    item.cancel.disabled = ["succeeded", "failed", "cancelled", "invalid"].includes(state);
  };
  const sendChunk = (item, offset, data) => new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    item.xhr = xhr;
    xhr.open("PUT", `${explorer.dataset.uploadUrl}/${encodeURIComponent(item.id)}/chunks?offset=${offset}`);
    xhr.setRequestHeader("Content-Type", "application/octet-stream");
    xhr.setRequestHeader("X-CSRF-Token", csrf);
    xhr.upload.onprogress = event => { if (event.lengthComputable) item.progress.value = offset + event.loaded; };
    xhr.onload = () => {
      try {
        const body = JSON.parse(xhr.responseText);
        if (xhr.status < 200 || xhr.status >= 300) throw new Error(body.error || `HTTP ${xhr.status}`);
        if (body.received !== offset + data.size) throw new Error("Storage accepted an unexpected byte offset");
        resolve(body.received);
      } catch (error) { reject(error); }
    };
    xhr.onerror = () => reject(new Error("Upload connection failed"));
    xhr.onabort = () => reject(new Error("Upload cancelled"));
    xhr.send(data);
  });
  const transfer = async item => {
    let offset = 0;
    while (offset < item.file.size) {
      if (item.cancelled || closed) throw new Error("Upload cancelled");
      offset = await sendChunk(item, offset, item.file.slice(offset, offset + 8 * 1024 * 1024));
      item.progress.value = offset;
    }
    item.xhr = null;
  };
  const cancelRemote = async item => {
    const record = await post(operationUrl(item.id) + "/cancel");
    if (!["cancelled", "completed", "succeeded"].includes(record.state)) {
      throw new Error(record.message || "Cancellation requires storage recovery.");
    }
    return record;
  };
  const acceptUpload = async (item, record) => {
    if (typeof record.id !== "string" || !record.id) throw new Error("Invalid upload identity");
    item.id = record.id;
    if (record.state !== "uploading") throw new Error(record.message || "Upload creation requires recovery.");
    if (closed || item.cancelled) {
      item.cancelled = true;
      await post(operationUrl(item.id) + "/cancel", {}, { keepalive: closed });
      throw new Error("Upload cancelled");
    }
  };
  const prepareUpload = async item => {
    if (item.id) {
      const ended = await cancelRemote(item);
      if (ended.state !== "cancelled") throw new Error("This upload already completed; refresh the folder instead of retrying.");
    }
    item.id = null;
    const path = [item.folder, item.file.name].filter(Boolean).join("/");
    const reviews = await replacement(path);
    if (!reviews) return false;
    const record = await post(explorer.dataset.uploadUrl, {
      folder: item.folder, name: item.file.name, size: item.file.size,
      confirmation: reviews[path] || "", confirm_replace: "yes",
    });
    await acceptUpload(item, record);
    return true;
  };
  const runUpload = async item => {
    item.cancelled = false;
    status(item, "transferring", "Reviewing destination...");
    try {
      if (!await prepareUpload(item)) { status(item, "cancelled", "Upload cancelled."); return; }
      status(item, "transferring", "Transferring original video bytes...");
      await transfer(item);
      await post(`${explorer.dataset.uploadUrl}/${encodeURIComponent(item.id)}/finish`);
      status(item, "verifying", "Verifying complete ADLS bytes; transfer is not yet complete.");
      const completed = await poll(item.id, () => item.cancelled, row => {
        item.text.textContent = row.message || "Verifying complete ADLS bytes...";
      });
      if (typeof completed.sha256 !== "string") throw new Error("Verified SHA-256 is missing");
      status(item, "succeeded", `Uploaded. SHA-256: ${completed.sha256}`);
    } catch (error) {
      item.xhr = null;
      status(item, item.cancelled ? "cancelled" : "failed", `Upload failed: ${error.message}`);
    }
  };
  const addFile = file => {
    const row = document.createElement("li");
    const name = document.createElement("p");
    name.textContent = `${file.name} -> ${chosenFolder || "allowed root"}`;
    const text = document.createElement("p");
    text.setAttribute("role", "status");
    const progress = document.createElement("progress");
    progress.max = file.size || 1;
    progress.value = 0;
    progress.setAttribute("aria-label", `Upload progress for ${file.name}`);
    const cancel = document.createElement("button");
    cancel.type = "button"; cancel.textContent = "Cancel upload"; cancel.className = "btn-secondary";
    const retry = document.createElement("button");
    retry.type = "button"; retry.textContent = "Retry"; retry.className = "btn-secondary";
    const item = { file, folder: chosenFolder, text, progress, cancel, retry, id: null, xhr: null };
    retry.addEventListener("click", () => withBusy(() => runUpload(item)));
    cancel.addEventListener("click", async () => {
      item.cancelled = true;
      if (item.xhr) item.xhr.abort();
      try {
        const record = item.id ? await cancelRemote(item) : { state: "cancelled" };
        if (record.state === "cancelled") status(item, "cancelled", "Upload cancelled; retry starts this file again.");
        else {
          item.cancelled = false;
          status(item, "succeeded", `Upload already completed. SHA-256: ${record.sha256}`);
        }
      } catch (error) { status(item, "failed", `Cannot cancel upload: ${error.message}`); }
    });
    row.append(name, text, progress, cancel, retry); results.append(row); items.push(item);
    const extension = file.name.slice(file.name.lastIndexOf(".")).toLowerCase();
    if (!picker.accept.split(",").includes(extension)) status(item, "invalid", "Unsupported video format.");
    else if (file.size <= 0 || file.size > limit) status(item, "invalid", "File is empty or exceeds the configured per-file upload limit.");
    else status(item, "pending", "Ready to upload to the current ADLS folder.");
  };
  picker.addEventListener("change", () => {
    for (const file of picker.files) addFile(file);
    controls();
  });
  form.addEventListener("submit", event => {
    event.preventDefault();
    withBusy(async () => {
      for (const item of items.filter(row => row.state === "pending")) {
        if (closed) break;
        await runUpload(item);
      }
      showMessage("Batch finished. Completed files remain in ADLS; retry only failed files. Refresh the folder to select uploaded footage.");
    });
  });
  controls();

  const reviewMove = async (nodes, folder, newName) => {
    const reviews = {};
    for (const node of nodes.filter(node => node.dataset.directory !== "true")) {
      const target = [folder, newName || node.value.split("/").pop()].filter(Boolean).join("/");
      const review = await replacement(target);
      if (review === null) return null;
      Object.assign(reviews, review);
    }
    return reviews;
  };
  const moveItems = async (nodes, rename) => {
    if (rename ? nodes.length !== 1 : nodes.length === 0) throw new Error("Select items; renaming requires exactly one item.");
    const value = await ask(rename ? "Rename selected item" : "Move selected items", nodes.map(node => node.value).join(", "), false, !rename);
    if (value === null) return;
    const paths = nodes.map(node => node.value);
    const folder = rename ? paths[0].split("/").slice(0, -1).join("/") : value;
    const reviews = await reviewMove(nodes, folder, rename ? value : "");
    if (reviews === null) return;
    const record = await post(explorer.dataset.moveUrl, { paths, destination: folder, rename: rename ? value : "", replacements: JSON.stringify(reviews), confirm_replace: "yes" });
    await poll(record.id, () => false, row => { showMessage(row.message || "Moving selected items..."); });
    showMessage("Move completed. Editable references updated; refresh the folder to view the new paths.");
  };
  const action = async name => {
    const nodes = Array.from(document.querySelectorAll("[data-intake-selection]:checked"))
      .filter(node => !node.closest("tr[hidden]"));
    if (name === "download") {
      if (!nodes.length || nodes.some(node => node.dataset.directory === "true")) throw new Error("Select supported videos for download.");
      const url = new URL(explorer.dataset.downloadUrl, location.origin);
      for (const node of nodes) url.searchParams.append("video", node.value);
      window.open(url.toString(), "_blank", "noopener");
    } else if (name === "create") {
      const path = await ask("Create folder", "Enter a new folder path within the configured ADLS root.");
      if (path !== null) { await post(explorer.dataset.folderUrl, { path }); showMessage("Folder created. Refresh the folder to view it."); }
    } else await moveItems(nodes, name === "rename");
  };
  for (const button of document.querySelectorAll("[data-intake-action]")) {
    button.addEventListener("click", () => withBusy(() => action(button.dataset.intakeAction)));
  }
  window.addEventListener("beforeunload", event => {
    if (items.some(item => item.state === "transferring")) { event.preventDefault(); event.returnValue = ""; }
  });
  window.addEventListener("pagehide", () => {
    closed = true;
    if (resolveModal) modal.close();
    for (const item of items.filter(row => row.state === "transferring")) {
      if (item.xhr) item.xhr.abort();
      if (item.id) post(operationUrl(item.id) + "/cancel", {}, { keepalive: true }).catch(error => { showMessage(`Cannot stop unfinished upload: ${error.message}`); });
    }
  });
}
