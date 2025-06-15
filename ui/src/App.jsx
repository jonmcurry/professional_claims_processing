import React, { useEffect, useState, useMemo } from 'react';

export default function App() {
  const [claims, setClaims] = useState([]);
  const [filter, setFilter] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: 'failed_at', direction: 'desc' });

  useEffect(() => {
    fetch('/api/failed_claims', { headers: { 'X-API-Key': 'test' } })
      .then((res) => res.json())
      .then((data) => setClaims(data))
      .catch((err) => console.error(err));
  }, []);

  const sortedClaims = useMemo(() => {
    return [...claims].sort((a, b) => {
      if (a[sortConfig.key] < b[sortConfig.key]) return sortConfig.direction === 'asc' ? -1 : 1;
      if (a[sortConfig.key] > b[sortConfig.key]) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });
  }, [claims, sortConfig]);

  const displayedClaims = useMemo(() => {
    const lower = filter.toLowerCase();
    return sortedClaims.filter((c) =>
      Object.values(c).some((v) => String(v).toLowerCase().includes(lower))
    );
  }, [sortedClaims, filter]);

  const requestSort = (key) => {
    let direction = 'asc';
    if (sortConfig.key === key && sortConfig.direction === 'asc') {
      direction = 'desc';
    }
    setSortConfig({ key, direction });
  };

  const getSortIndicator = (key) => {
    if (sortConfig.key !== key) return '';
    return sortConfig.direction === 'asc' ? ' \u25B2' : ' \u25BC';
  };

  return (
    <div style={{ padding: '1rem' }}>
      <h1>Failed Claims</h1>
      <input
        placeholder="Filter..."
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        style={{ marginBottom: '1rem' }}
      />
      <table border="1" cellPadding="5" cellSpacing="0">
        <thead>
          <tr>
            <th onClick={() => requestSort('claim_id')}>Claim ID{getSortIndicator('claim_id')}</th>
            <th onClick={() => requestSort('failure_reason')}>Reason{getSortIndicator('failure_reason')}</th>
            <th onClick={() => requestSort('failed_at')}>Failed At{getSortIndicator('failed_at')}</th>
          </tr>
        </thead>
        <tbody>
          {displayedClaims.map((c) => (
            <tr key={c.claim_id}>
              <td>{c.claim_id}</td>
              <td>{c.failure_reason}</td>
              <td>{c.failed_at}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
