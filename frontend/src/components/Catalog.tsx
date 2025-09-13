import { useState, useEffect } from 'react';
import EditModal from './EditModal';
import styled from 'styled-components';
import { format } from 'date-fns';
import { catalogApi } from '../services/api';
import type { CatalogEntry } from '../services/api';

const CatalogContainer = styled.div`
  padding: 20px;
  font-family: monospace;
`;

const Header = styled.div`
  border: 2px solid #333;
  padding: 15px;
  text-align: center;
  margin-bottom: 30px;
  font-size: 1.5em;
  font-weight: bold;
  background-color: #f5f5f5;
  border-radius: 4px;
`;

const FiltersContainer = styled.div`
  display: flex;
  gap: 15px;
  margin-bottom: 20px;
  padding: 15px;
  background: #f5f5f5;
  border-radius: 4px;
`;

const Select = styled.select`
  padding: 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-family: monospace;
`;

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-top: 20px;
  background: white;
`;

const Th = styled.th`
  padding: 12px;
  text-align: left;
  border-bottom: 2px solid #333;
  background: #f5f5f5;
`;

const Td = styled.td`
  padding: 12px;
  border-bottom: 1px solid #ddd;
`;

const StatusBadge = styled.span<{ $status: string }>`
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  background: ${props => {
    switch (props.$status) {
      case 'full_load': return '#e3f2fd';
      case 'incremental': return '#e8f5e9';
      case 'error': return '#ffebee';
      case 'no_data': return '#fff3e0';
      default: return '#f5f5f5';
    }
  }};
  color: ${props => {
    switch (props.$status) {
      case 'full_load': return '#1976d2';
      case 'incremental': return '#2e7d32';
      case 'error': return '#c62828';
      case 'no_data': return '#ef6c00';
      default: return '#333';
    }
  }};
`;

const ActiveBadge = styled.span<{ $active: boolean }>`
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  background: ${props => props.$active ? '#e8f5e9' : '#ffebee'};
  color: ${props => props.$active ? '#2e7d32' : '#c62828'};
`;

const ActionButton = styled.button`
  padding: 4px 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background: white;
  cursor: pointer;
  font-family: monospace;
  
  &:hover {
    background: #f5f5f5;
  }
`;

// Loading state indicator
const LoadingOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(255, 255, 255, 0.8);
  display: flex;
  justify-content: center;
  align-items: center;
  font-size: 1.2em;
  z-index: 1000;
`;

// Error message styling
const ErrorMessage = styled.div`
  padding: 15px;
  margin: 20px 0;
  background-color: #ffebee;
  color: #c62828;
  border-radius: 4px;
  border: 1px solid #ef9a9a;
`;

const Catalog = () => {
  const [filter, setFilter] = useState({
    engine: '',
    status: '',
    active: ''
  });

  const [data, setData] = useState<CatalogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedEntry, setSelectedEntry] = useState<CatalogEntry | null>(null);

  // Cargar datos del catálogo
  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const entries = await catalogApi.getCatalogEntries();
        setData(entries);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading catalog data');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    // Actualizar cada 30 segundos
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  // Manejar cambio de estado activo
  const handleToggleActive = async (entry: CatalogEntry) => {
    try {
      setLoading(true);
      await catalogApi.updateEntryStatus(
        entry.schema_name,
        entry.table_name,
        entry.db_engine,
        !entry.active
      );
      // Recargar datos después de la actualización
      const entries = await catalogApi.getCatalogEntries();
      setData(entries);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error updating entry status');
    } finally {
      setLoading(false);
    }
  };

  // Forzar sincronización completa
  const handleEdit = async (updatedEntry: CatalogEntry) => {
    try {
      setLoading(true);
      await catalogApi.updateEntry(updatedEntry);
      // Recargar datos después de la actualización
      const entries = await catalogApi.getCatalogEntries();
      setData(entries);
      setSelectedEntry(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error updating entry');
    } finally {
      setLoading(false);
    }
  };

  const handleForceSync = async (entry: CatalogEntry) => {
    try {
      setLoading(true);
      await catalogApi.triggerFullSync(
        entry.schema_name,
        entry.table_name,
        entry.db_engine
      );
      // Recargar datos después de la sincronización
      const entries = await catalogApi.getCatalogEntries();
      setData(entries);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error triggering full sync');
    } finally {
      setLoading(false);
    }
  };

  return (
    <CatalogContainer>
      {loading && <LoadingOverlay>Loading...</LoadingOverlay>}
      
      <Header>
        DataLake Catalog Manager
      </Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}

      <FiltersContainer>
        <Select 
          value={filter.engine}
          onChange={(e) => setFilter({...filter, engine: e.target.value})}
        >
          <option value="">All Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MongoDB">MongoDB</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MariaDB">MariaDB</option>
        </Select>

        <Select
          value={filter.status}
          onChange={(e) => setFilter({...filter, status: e.target.value})}
        >
          <option value="">All Status</option>
          <option value="full_load">Full Load</option>
          <option value="incremental">Incremental</option>
          <option value="error">Error</option>
          <option value="no_data">No Data</option>
        </Select>

        <Select
          value={filter.active}
          onChange={(e) => setFilter({...filter, active: e.target.value})}
        >
          <option value="">All States</option>
          <option value="true">Active</option>
          <option value="false">Inactive</option>
        </Select>
      </FiltersContainer>

      <Table>
        <thead>
          <tr>
            <Th>Schema.Table</Th>
            <Th>Engine</Th>
            <Th>Status</Th>
            <Th>Active</Th>
            <Th>Last Sync</Th>
            <Th>Sync Column</Th>
            <Th>Offset</Th>
            <Th>Cluster</Th>
            <Th>Actions</Th>
          </tr>
        </thead>
        <tbody>
          {data.map((entry, index) => (
            <tr key={index}>
              <Td>{entry.schema_name}.{entry.table_name}</Td>
              <Td>{entry.db_engine}</Td>
              <Td>
                <StatusBadge $status={entry.status}>
                  {entry.status}
                </StatusBadge>
              </Td>
              <Td>
                <ActiveBadge $active={entry.active}>
                  {entry.active ? 'Active' : 'Inactive'}
                </ActiveBadge>
              </Td>
              <Td>{format(new Date(entry.last_sync_time), 'yyyy-MM-dd HH:mm:ss')}</Td>
              <Td>{entry.last_sync_column}</Td>
              <Td>{entry.last_offset}</Td>
              <Td>{entry.cluster_name}</Td>
              <Td>
                <ActionButton onClick={() => setSelectedEntry(entry)}>
                  ✎ Edit
                </ActionButton>
              </Td>
            </tr>
          ))}
        </tbody>
      </Table>

      {selectedEntry && (
        <EditModal
          entry={selectedEntry}
          onClose={() => setSelectedEntry(null)}
          onSave={handleEdit}
        />
      )}
    </CatalogContainer>
  );
};

export default Catalog;