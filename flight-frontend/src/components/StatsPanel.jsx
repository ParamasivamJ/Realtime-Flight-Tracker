const StatsPanel = ({ stats }) => {
  if (!stats) return null

  return (
    <div className="stats-panel">
      <div className="stat-item">
        <span className="stat-value">{stats.total_flights}</span>
        <span className="stat-label">Active Flights</span>
      </div>
      <div className="stat-item">
        <span className="stat-value">{stats.active_countries}</span>
        <span className="stat-label">Countries</span>
      </div>
      <div className="stat-item">
        <span className="stat-value">{Math.round(stats.avg_speed)}</span>
        <span className="stat-label">Avg Speed (kts)</span>
      </div>
      <div className="stat-item">
        <span className="stat-value">{Math.round(stats.avg_altitude)}</span>
        <span className="stat-label">Avg Altitude (ft)</span>
      </div>
      <div className="stat-item">
        <span className="stat-value">{stats.climbing_flights}</span>
        <span className="stat-label">Climbing</span>
      </div>
      <div className="stat-item">
        <span className="stat-value">{stats.descending_flights}</span>
        <span className="stat-label">Descending</span>
      </div>
    </div>
  )
}

export default StatsPanel