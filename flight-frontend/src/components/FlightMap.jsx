import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'

// Fix for default markers in React-Leaflet
delete L.Icon.Default.prototype._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
})

// Create custom plane icon
const createPlaneIcon = () => {
  return new L.DivIcon({
    html: `
      <div style="
        background: #4CAF50;
        width: 24px;
        height: 24px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-size: 12px;
        font-weight: bold;
        border: 2px solid white;
        box-shadow: 0 2px 4px rgba(0,0,0,0.3);
      ">✈️</div>
    `,
    iconSize: [24, 24],
    iconAnchor: [12, 12],
  })
}

const FlightMap = ({ flights }) => {
  // Default center if no flights
  const defaultCenter = [20, 0]
  const defaultZoom = 2

  // Calculate center based on flights if available
  const mapCenter = flights.length > 0 
    ? [flights[0].latitude, flights[0].longitude] 
    : defaultCenter

  return (
    <MapContainer
      center={mapCenter}
      zoom={defaultZoom}
      style={{ height: '100%', width: '100%' }}
      scrollWheelZoom={true}
    >
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      />
      {flights.map((flight, index) => (
        <Marker
          key={flight.icao24 || index}
          position={[flight.latitude, flight.longitude]}
          icon={createPlaneIcon()}
        >
          <Popup>
            <div style={{ minWidth: '200px' }}>
              <h3 style={{ margin: '0 0 10px 0', color: '#333' }}>
                Flight {flight.callsign}
              </h3>
              <p style={{ margin: '5px 0' }}>
                <strong>Country:</strong> {flight.origin_country}
              </p>
              <p style={{ margin: '5px 0' }}>
                <strong>Altitude:</strong> {Math.round(flight.altitude)} ft
              </p>
              <p style={{ margin: '5px 0' }}>
                <strong>Speed:</strong> {Math.round(flight.velocity)} kts
              </p>
              <p style={{ margin: '5px 0' }}>
                <strong>Heading:</strong> {Math.round(flight.heading)}°
              </p>
              <p style={{ margin: '5px 0' }}>
                <strong>Status:</strong> {
                  flight.vertical_rate > 0 ? '🔼 Climbing' : 
                  flight.vertical_rate < 0 ? '🔽 Descending' : '➡️ Cruising'
                }
              </p>
            </div>
          </Popup>
        </Marker>
      ))}
    </MapContainer>
  )
}

export default FlightMap