// 🔥 Market Watch Dashboard Script

async function fetchMarketData() {
  try {
    // Fetch gainers and losers from your own backend
    const gainersResponse = await fetch('/api/market/gainers');
    const losersResponse = await fetch('/api/market/losers');

    const gainers = await gainersResponse.json();
    const losers = await losersResponse.json();

    // Update both tables
    updateTable('gainersTableBody', gainers);
    updateTable('losersTableBody', losers);

  } catch (error) {
    console.error("Market Watch Error:", error);
    // If any error, show error message in both tables
    document.getElementById('gainersTableBody').innerHTML = `
      <tr><td colspan="3" class="text-center text-muted">Error fetching data</td></tr>`;
    document.getElementById('losersTableBody').innerHTML = `
      <tr><td colspan="3" class="text-center text-muted">Error fetching data</td></tr>`;
  }
}

function updateTable(tableId, stocks) {
  const tableBody = document.getElementById(tableId);
  tableBody.innerHTML = ""; // Clear previous data

  if (Array.isArray(stocks) && stocks.length > 0) {
    stocks.slice(0, 10).forEach(stock => {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td><b>${stock.symbol}</b></td>
        <td>₹${parseFloat(stock.price).toFixed(2)}</td>
        <td class="${parseFloat(stock.pChange) >= 0 ? 'text-success' : 'text-danger'}">
          ${parseFloat(stock.pChange).toFixed(2)}%
        </td>
      `;
      tableBody.appendChild(row);
    });
  } else {
    tableBody.innerHTML = `
      <tr><td colspan="3" class="text-center text-muted">No data available</td></tr>`;
  }
}

// 🔄 Load initially and auto-refresh every 60 seconds
fetchMarketData();
setInterval(fetchMarketData, 60000);
