(function () {
  function handleMoneylineEvent(event) {
    const target = event.target;
    if (!target || !target.classList.contains("moneyline-link")) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation();

    const rowIndex = target.getAttribute("data-row-index");
    if (rowIndex == null) {
      return;
    }

    const input = document.getElementById("moneyline-link-input");
    if (!input) {
      return;
    }

    const payload = {
      row: parseInt(rowIndex, 10),
      timestamp: Date.now(),
    };

    input.value = JSON.stringify(payload);
    const inputEvent = new Event("input", { bubbles: true });
    input.dispatchEvent(inputEvent);
  }

  document.addEventListener("pointerdown", handleMoneylineEvent, true);
  document.addEventListener("click", handleMoneylineEvent, true);
})();
