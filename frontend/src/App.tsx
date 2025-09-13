import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './components/Dashboard'
import Catalog from './components/Catalog'
import Monitor from './components/Monitor'
import Quality from './components/Quality'
import Governance from './components/Governance'
import Config from './components/Config'

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="catalog" element={<Catalog />} />
          <Route path="monitor" element={<Monitor />} />
          <Route path="quality" element={<Quality />} />
                <Route path="governance" element={<Governance />} />
                <Route path="config" element={<Config />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default App