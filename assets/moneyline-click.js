(function() {
  if (!window.dash_moneyline_click_handler) {
    window.dash_moneyline_click_handler = function(event) {
      const target = event.target;
      if (target && target.classList.contains('moneyline-link')) {
        event.stopPropagation();
        const parentCell = target.closest('td[data-dash-column="moneyline_display"]');
        if (!parentCell) {
          return;
        }
        const rowIndex = parentCell.parentElement.getAttribute('data-dash-row');
        const table = parentCell.closest('table');
        if (!rowIndex || !table) {
          return;
        }
        const tableId = table.getAttribute('data-dash-table');
        if (!tableId) {
          return;
        }
        const customEvent = new CustomEvent('moneyline-link-click', {
          detail: {
            tableId: tableId,
            row: parseInt(rowIndex, 10),
          },
          bubbles: true,
        });
        document.dispatchEvent(customEvent);
      }
    };
    document.addEventListener('click', window.dash_moneyline_click_handler, true);
  }
})();
