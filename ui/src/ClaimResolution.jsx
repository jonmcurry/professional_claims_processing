import React, { useEffect, useState } from 'react';

export default function ClaimResolution() {
  const [claims, setClaims] = useState([]);
  const [user, setUser] = useState('');

  useEffect(() => {
    fetch('/api/failed_claims', { headers: { 'X-API-Key': 'test' } })
      .then((res) => res.json())
      .then((data) => setClaims(data))
      .catch((err) => console.error(err));
  }, []);

  const assignClaim = (id) => {
    fetch('/api/assign_failed_claim', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': 'test' },
      body: JSON.stringify({ claim_id: id, user }),
    }).then(() => alert('Assigned'));
  };

  const resolveClaim = (id) => {
    const action = prompt('Resolution action?');
    if (!action) return;
    const notes = prompt('Notes?') || '';
    fetch('/api/resolve_failed_claim', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': 'test' },
      body: JSON.stringify({ claim_id: id, action, notes }),
    }).then(() => setClaims((prev) => prev.filter((c) => c.claim_id !== id)));
  };

  return (
    <div style={{ padding: '1rem' }}>
      <h1>Resolve Failed Claims</h1>
      <input
        placeholder="User..."
        value={user}
        onChange={(e) => setUser(e.target.value)}
        style={{ marginBottom: '1rem' }}
      />
      <table border="1" cellPadding="5" cellSpacing="0">
        <thead>
          <tr>
            <th>Claim ID</th>
            <th>Reason</th>
            <th>Failed At</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {claims.map((c) => (
            <tr key={c.claim_id}>
              <td>{c.claim_id}</td>
              <td>{c.failure_reason}</td>
              <td>{c.failed_at}</td>
              <td>
                <button onClick={() => assignClaim(c.claim_id)}>Assign</button>
                <button onClick={() => resolveClaim(c.claim_id)}>Resolve</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
