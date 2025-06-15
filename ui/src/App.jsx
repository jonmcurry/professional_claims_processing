import React from 'react';
import FailedClaims from './FailedClaims';
import Dashboard from './Dashboard';

export default function App() {
  const path = window.location.pathname;
  if (path.startsWith('/dashboard')) {
    return <Dashboard />;
  }
  return <FailedClaims />;
}
