const $ = (id) => document.getElementById(id);
let state,
  selected,
  busy = false;
const names = {
  none: "Normal",
  before_receive: "Before receiver",
  after_receive: "Lost acknowledgement",
};
function element(tag, text, className) {
  const el = document.createElement(tag);
  el.textContent = text;
  if (className) el.className = className;
  return el;
}
function message(text, error = false) {
  $("message").textContent = text;
  $("message").classList.toggle("error", error);
  $("create-message").textContent = text;
  $("create-message").classList.toggle("error", error);
}
async function api(path, body) {
  const response = await fetch(
    path,
    body === undefined
      ? {}
      : {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
  );
  const data = await response.json();
  if (!response.ok) throw Error(data.error || "Request failed.");
  return data;
}
function newKey() {
  $("key").value = "shipment-" + crypto.randomUUID();
}
function render() {
  if (!state.outbox.some((e) => e.id === selected))
    selected = state.outbox.at(-1)?.id;
  $("pending").textContent =
    state.outbox.filter((e) => !e.acknowledged).length + " pending";
  $("received").textContent = state.inbox.length + " receipts";
  $("outbox").replaceChildren();
  for (const event of [...state.outbox].reverse()) {
    const b = element("button", "", "parcel");
    b.type = "button";
    b.dataset.event = event.id;
    b.disabled = busy;
    b.setAttribute("aria-pressed", String(event.id === selected));
    b.append(
      element("span", "#" + event.shipmentId, "parcel-number"),
      element("strong", event.memo),
      element(
        "small",
        event.acknowledged ? "Acknowledged" : "Awaiting acknowledgement",
      ),
    );
    b.onclick = () => {
      if (busy) return;
      selected = event.id;
      render();
      $("outbox")
        .querySelector(`[data-event="${event.id}"]`)
        ?.focus({ preventScroll: true });
    };
    $("outbox").append(b);
  }
  if (!state.outbox.length)
    $("outbox").append(
      element("p", "No shipments. Use Create shipment to add one.", "empty"),
    );
  $("totals").replaceChildren();
  for (const row of state.totals) {
    const el = element("div", "", "total");
    el.append(element("span", row.sku), element("b", String(row.quantity)));
    $("totals").append(el);
  }
  if (!state.totals.length)
    $("totals").append(element("p", "No inventory received yet.", "empty"));
  $("inbox").replaceChildren(
    ...[...state.inbox]
      .reverse()
      .map((e) =>
        element("div", "#" + e.shipmentId + " · " + e.memo, "receipt"),
      ),
  );
  const event = state.outbox.find((e) => e.id === selected);
  const received = !!event && state.inbox.some((e) => e.id === event.id);
  const acknowledged = !!event?.acknowledged;
  for (const [id, done] of [
    ["sender-stage", !!event],
    ["receiver-stage", received],
    ["ack-stage", acknowledged],
  ]) {
    $(id).classList.toggle("complete", done);
    $(id).setAttribute(
      "aria-label",
      `${$(id).querySelector("strong").textContent}: ${done ? "complete" : "pending"}`,
    );
  }
  $("track-progress").style.width = acknowledged
    ? "100%"
    : received
      ? "50%"
      : "0%";
  $("selected-title").textContent = event
    ? "#" + event.shipmentId + " · " + event.memo
    : "Create a shipment to begin.";
  $("selected-id").textContent = event?.id || "";
  $("send").disabled = busy || !event;
  $("attempts").replaceChildren();
  for (const attempt of [...state.attempts].reverse()) {
    const tr = document.createElement("tr");
    const shipment = state.outbox.find((e) => e.id === attempt.eventId);
    for (const text of [
      attempt.id,
      "#" + shipment?.shipmentId,
      names[attempt.fault],
      attempt.status.replaceAll("_", " "),
      attempt.duplicate === 1
        ? "Duplicate · unchanged"
        : attempt.duplicate === 0
          ? "Applied once"
          : "Not recorded",
    ])
      tr.append(element("td", String(text)));
    $("attempts").append(tr);
  }
  $("audit").textContent = state.audit.ok
    ? "Replay audit passes"
    : "Audit mismatch";
}
async function refresh() {
  state = await api("/api/state");
  render();
}
async function action(fn) {
  if (busy) return;
  busy = true;
  document
    .querySelectorAll("button,input,select,textarea")
    .forEach((b) => (b.disabled = true));
  try {
    await fn();
  } catch (error) {
    message(error.message, true);
  } finally {
    busy = false;
    document
      .querySelectorAll("button,input,select,textarea")
      .forEach((b) => (b.disabled = false));
    if (state) render();
  }
}
$("create-open").onclick = () => $("create-dialog").showModal();
$("create-close").onclick = () => $("create-dialog").close();
$("create").onsubmit = (e) => {
  e.preventDefault();
  action(async () => {
    const result = await api("/api/shipments", {
      key: $("key").value,
      memo: $("memo").value,
      items: JSON.parse($("items").value),
    });
    selected = result.event.id;
    await refresh();
    message(
      result.replayed
        ? "Existing shipment returned. No second event was created."
        : "Shipment and outbox event committed together.",
    );
  });
};
$("deliver").onsubmit = (e) => {
  e.preventDefault();
  if (!selected) return;
  action(async () => {
    const result = await api("/api/events/" + selected + "/deliver", {
      fault: $("fault").value,
    });
    await refresh();
    message(
      result.status === "ack_lost"
        ? "Receiver committed. Sender has no new acknowledgement. Retry to observe deduplication."
        : result.status === "not_received"
          ? "Delivery stopped before the receiver. Inventory is unchanged."
          : result.duplicate
            ? "Acknowledged duplicate. Receiver quantities are unchanged."
            : "Receiver committed and sender recorded the acknowledgement.",
    );
  });
};
$("refresh").onclick = () =>
  action(async () => {
    await refresh();
    message("Both database snapshots refreshed.");
  });
$("new-key").onclick = newKey;
$("export").onclick = () => {
  if (!state) return;
  const blob = new Blob([JSON.stringify(state, null, 2) + "\n"], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "parcel-relay-evidence.json";
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
};
newKey();
action(async () => {
  await refresh();
  message("Ready. Create a shipment, then choose a delivery scenario.");
});
