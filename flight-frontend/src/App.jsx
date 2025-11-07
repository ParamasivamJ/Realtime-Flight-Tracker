import { useState, useEffect } from 'react'
import FlightMap from './components/FlightMap'
import StatsPanel from './components/StatsPanel'
import './App.css'

function App() {
  const [flights, setFlights] = useState([])
  const [stats, setStats] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  const fetchData = async () => {
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000'
      console.log('Fetching from:', API_BASE)
      
      const [flightsResponse, statsResponse] = await Promise.all([
        fetch(`${API_BASE}/flights/current`),
        fetch(`${API_BASE}/flights/stats`)
      ])
      
      if (!flightsResponse.ok) throw new Error('Failed to fetch flights')
      if (!statsResponse.ok) throw new Error('Failed to fetch stats')
      
      const flightsData = await flightsResponse.json()
      const statsData = await statsResponse.json()
      
      setFlights(flightsData)
      setStats(statsData)
      setLoading(false)
      setError(null)
    } catch (err) {
      console.error('Error fetching data:', err)
      setError(err.message)
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 10000)
    return () => clearInterval(interval)
  }, [])

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner">✈️</div>
        <h2>Loading flight data...</h2>
      </div>
    )
  }

  if (error) {
    return (
      <div className="error">
        <h2>Connection Error</h2>
        <p>{error}</p>
        <button onClick={fetchData}>Retry</button>
      </div>
    )
  }

  // Generate mock data if no real data
  const displayFlights = flights.length > 0 ? flights : generateMockFlights()
  const displayStats = stats && stats.total_flights > 0 ? stats : {
    total_flights: displayFlights.length,
    active_countries: 12,
    avg_speed: 512,
    avg_altitude: 35000,
    climbing_flights: 8,
    descending_flights: 6
  }

  return (
    <div className="app">
      <div className="sidebar">
        <div className="header">
          <h1>✈️ Real-Time Flight Tracker</h1>
          <div className="last-updated">
            Last updated: {new Date().toLocaleTimeString()}
            {flights.length === 0 && <div style={{ color: '#ffa500', marginTop: '5px' }}>Using demo data</div>}
          </div>
        </div>
        
        <StatsPanel stats={displayStats} />
        
        <div className="flight-list">
          <h3>Active Flights ({displayFlights.length})</h3>
          <div className="flights">
            {displayFlights.slice(0, 8).map((flight, index) => (
              <div key={flight.icao24 || index} className="flight-item">
                <div className="flight-header">
                  <strong>{flight.callsign}</strong>
                  <span className="country">{flight.origin_country}</span>
                </div>
                <div className="flight-details">
                  <small>Speed: {Math.round(flight.velocity)} kts</small>
                  <small>Alt: {Math.round(flight.altitude)} ft</small>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
      
      <div className="map-container">
        <FlightMap flights={displayFlights} />
      </div>
    </div>
  )
}

// Mock data generator
function generateMockFlights() {
  const countries = ['United States', 'Germany', 'France', 'United Kingdom', 'China', 'Japan', 'Canada', 'Brazil', 'Australia', 'India']
  const callsigns = ['UAL', 'DAL', 'AAL', 'BAW', 'AFR', 'LUF', 'KLM', 'EMI', 'QFA', 'SIA']
  
  // Generate flights over realistic regions
  const regions = [
    { center: [40.0, -100.0], spread: 15 },  // North America
    { center: [50.0, 10.0], spread: 20 },    // Europe
    { center: [35.0, 100.0], spread: 15 },   // Asia
    { center: [-25.0, 135.0], spread: 10 },  // Australia
  ]
  
  const mockFlights = []
  let flightId = 0
  
  regions.forEach(region => {
    for (let i = 0; i < 6; i++) {
      const lat = region.center[0] + (Math.random() - 0.5) * region.spread
      const lng = region.center[1] + (Math.random() - 0.5) * region.spread * 2
      
      mockFlights.push({
        icao24: `mock${flightId.toString().padStart(3, '0')}`,
        callsign: `${callsigns[flightId % callsigns.length]}${100 + flightId}`,
        origin_country: countries[flightId % countries.length],
        longitude: parseFloat(lng.toFixed(4)),
        latitude: parseFloat(lat.toFixed(4)),
        altitude: 30000 + Math.random() * 10000,
        velocity: 400 + Math.random() * 200,
        heading: Math.random() * 360,
        vertical_rate: (Math.random() - 0.5) * 40
      })
      flightId++
    }
  })
  
  return mockFlights
}

export default App