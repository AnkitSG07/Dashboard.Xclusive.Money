async function fetchMarketData() {
  try {
    const gainersResponse = await fetch('/api/market/gainers');
    const losersResponse = await fetch('/api/market/losers');

    const gainers = await gainersResponse.json();
    const losers = await losersResponse.json();

    updateTable('gainersTableBody', gainers);
    updateTable('losersTableBody', losers);

  } catch (error) {
    console.error("Market Watch Error:", error);
    document.getElementById('gainersTableBody').innerHTML = `<tr><td colspan="3" class="text-center text-muted">Error fetching data</td></tr>`;
    document.getElementById('losersTableBody').innerHTML = `<tr><td colspan="3" class="text-center text-muted">Error fetching data</td></tr>`;
  }
}

function updateTable(tableId, stocks) {
  const tableBody = document.getElementById(tableId);
  tableBody.innerHTML = "";

  if (stocks.length > 0) {
    stocks.slice(0, 10).forEach(stock => {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td><b>${stock.symbol}</b></td>
        <td>${stock.lastPrice}</td>
        <td class="${parseFloat(stock.pChange) > 0 ? 'text-success' : 'text-danger'}">
          ${parseFloat(stock.pChange).toFixed(2)}%
        </td>
      `;
      tableBody.appendChild(row);
    });
  } else {
    tableBody.innerHTML = `<tr><td colspan="3" class="text-center text-muted">No data available</td></tr>`;
  }
}

// 🔄 Initial + auto refresh every 60s
fetchMarketData();
setInterval(fetchMarketData, 60000);
