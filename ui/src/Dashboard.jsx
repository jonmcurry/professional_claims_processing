import React, { useEffect, useState } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  ArcElement,
  Tooltip,
  Legend,
} from 'chart.js';
import { Bar, Line, Pie } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  ArcElement,
  Tooltip,
  Legend
);

export default function Dashboard() {
  const [data, setData] = useState(null);

  useEffect(() => {
    fetch('/compliance/dashboard', { headers: { 'X-API-Key': 'test' } })
      .then((res) => res.json())
      .then((d) => setData(d))
      .catch((err) => console.error(err));
  }, []);

  if (!data) return <div>Loading...</div>;

  const failureLabels = data.failure_patterns.map((f) => f[0]);
  const failureCounts = data.failure_patterns.map((f) => f[1]);

  const trendLabels = data.processing_trends.map((t) => t.date);
  const processed = data.processing_trends.map((t) => t.processed);
  const failed = data.processing_trends.map((t) => t.failed);

  const catLabels = Object.keys(data.revenue_impact.by_category);
  const catValues = Object.values(data.revenue_impact.by_category);

  return (
    <div style={{ padding: '1rem' }}>
      <h1>Compliance Dashboard</h1>
      <h2>Failure Patterns</h2>
      <Bar
        data={{
          labels: failureLabels,
          datasets: [
            {
              label: 'Failures',
              data: failureCounts,
              backgroundColor: 'rgba(255,99,132,0.5)',
            },
          ],
        }}
      />
      <h2>Processing Trends (30d)</h2>
      <Line
        data={{
          labels: trendLabels,
          datasets: [
            {
              label: 'Processed',
              data: processed,
              borderColor: 'green',
            },
            {
              label: 'Failed',
              data: failed,
              borderColor: 'red',
            },
          ],
        }}
      />
      <h2>Revenue Impact</h2>
      <Pie
        data={{
          labels: catLabels,
          datasets: [
            {
              data: catValues,
              backgroundColor: [
                '#ff6384',
                '#36a2eb',
                '#ffcd56',
                '#4bc0c0',
                '#9966ff',
              ],
            },
          ],
        }}
      />
      <p>Total Potential Revenue Loss: ${data.revenue_impact.total.toFixed(2)}</p>
    </div>
  );
}
