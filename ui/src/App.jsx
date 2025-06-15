import React from 'react';
import FailedClaims from './FailedClaims';
import Dashboard from './Dashboard';
import ClaimResolution from './ClaimResolution';

export default function App() {
  const path = window.location.pathname;
  if (path.startsWith('/dashboard')) {
    return <Dashboard />;
  }
  if (path.startsWith('/resolve')) {
    return <ClaimResolution />;
  }
  return <FailedClaims />;
}
