import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { governanceApi } from '../services/api';

const GovernanceContainer = styled.div`
  background-color: white;
  color: #333;
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
  white-space: nowrap;
`;

const Td = styled.td`
  padding: 12px;
  border-bottom: 1px solid #ddd;
`;

const Badge = styled.span<{ type: string }>`
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  font-weight: 500;
  background-color: ${props => {
    switch (props.type) {
      // Health Status
      case 'HEALTHY': return '#e8f5e9';
      case 'WARNING': return '#fff3e0';
      case 'CRITICAL': return '#ffebee';
      // Access Frequency
      case 'HIGH': return '#e3f2fd';
      case 'MEDIUM': return '#f3e5f5';
      case 'LOW': return '#fafafa';
      // Sensitivity Level
      case 'HIGH_SENSITIVITY': return '#ffebee';
      case 'MEDIUM_SENSITIVITY': return '#fff3e0';
      case 'LOW_SENSITIVITY': return '#f1f8e9';
      // Data Category
      case 'TRANSACTIONAL': return '#e8eaf6';
      case 'ANALYTICAL': return '#f3e5f5';
      case 'REFERENCE': return '#e0f2f1';
      default: return '#f5f5f5';
    }
  }};
  color: ${props => {
    switch (props.type) {
      // Health Status
      case 'HEALTHY': return '#2e7d32';
      case 'WARNING': return '#ef6c00';
      case 'CRITICAL': return '#c62828';
      // Access Frequency
      case 'HIGH': return '#1565c0';
      case 'MEDIUM': return '#6a1b9a';
      case 'LOW': return '#616161';
      // Sensitivity Level
      case 'HIGH_SENSITIVITY': return '#c62828';
      case 'MEDIUM_SENSITIVITY': return '#ef6c00';
      case 'LOW_SENSITIVITY': return '#558b2f';
      // Data Category
      case 'TRANSACTIONAL': return '#3949ab';
      case 'ANALYTICAL': return '#7b1fa2';
      case 'REFERENCE': return '#00796b';
      default: return '#757575';
    }
  }};
`;

const QualityScore = styled.span<{ score: number }>`
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  font-weight: 500;
  background-color: ${props => {
    if (props.score >= 90) return '#e8f5e9';
    if (props.score >= 70) return '#f1f8e9';
    if (props.score >= 50) return '#fff3e0';
    return '#ffebee';
  }};
  color: ${props => {
    if (props.score >= 90) return '#2e7d32';
    if (props.score >= 70) return '#558b2f';
    if (props.score >= 50) return '#ef6c00';
    return '#c62828';
  }};
`;

const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  margin-top: 20px;
  padding: 15px;
`;

const PageButton = styled.button<{ active?: boolean }>`
  padding: 5px 10px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background: ${props => props.active ? '#333' : 'white'};
  color: ${props => props.active ? 'white' : '#333'};
  cursor: pointer;
  font-family: monospace;
  
  &:hover {
    background: ${props => props.active ? '#333' : '#f5f5f5'};
  }
  
  &:disabled {
    cursor: not-allowed;
    opacity: 0.5;
  }
`;

const formatDate = (date: string) => {
  if (!date) return '-';
  return new Date(date).toLocaleString();
};

const formatSize = (mb: number | null | undefined) => {
  if (mb == null) return '-';
  const size = Number(mb);
  if (isNaN(size)) return '-';
  if (size >= 1024) {
    return `${(size / 1024).toFixed(2)} GB`;
  }
  return `${size.toFixed(2)} MB`;
};

const formatNumber = (num: number) => {
  return num?.toLocaleString() || '0';
};

const Governance = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<any[]>([]);
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 10
  });

  const [filter, setFilter] = useState({
    engine: '',
    category: '',
    health: '',
    domain: '',
    sensitivity: ''
  });

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await governanceApi.getGovernanceData({
          page,
          limit: 10,
          ...filter
        });
        setData(response.data);
        setPagination(response.pagination);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading governance data');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, [page, filter]);

  return (
    <GovernanceContainer>
      <Header>
        Data Governance Catalog
      </Header>

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
          value={filter.category}
          onChange={(e) => setFilter({...filter, category: e.target.value})}
        >
          <option value="">All Categories</option>
          <option value="TRANSACTIONAL">Transactional</option>
          <option value="ANALYTICAL">Analytical</option>
          <option value="REFERENCE">Reference</option>
        </Select>

        <Select
          value={filter.health}
          onChange={(e) => setFilter({...filter, health: e.target.value})}
        >
          <option value="">All Health Status</option>
          <option value="HEALTHY">Healthy</option>
          <option value="WARNING">Warning</option>
          <option value="CRITICAL">Critical</option>
        </Select>

        <Select
          value={filter.sensitivity}
          onChange={(e) => setFilter({...filter, sensitivity: e.target.value})}
        >
          <option value="">All Sensitivity</option>
          <option value="LOW">Low</option>
          <option value="MEDIUM">Medium</option>
          <option value="HIGH">High</option>
        </Select>
      </FiltersContainer>

      {loading && (
        <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
          Loading governance data...
        </div>
      )}

      {error && (
        <div style={{ color: 'red', padding: '20px', textAlign: 'center' }}>
          {error}
        </div>
      )}

      {!loading && !error && (
        <>
          <Table>
            <thead>
              <tr>
                <Th>Schema.Table</Th>
                <Th>Engine</Th>
                <Th>Category</Th>
                <Th>Domain</Th>
                <Th>Health</Th>
                <Th>Sensitivity</Th>
                <Th>Quality</Th>
                <Th>Size</Th>
                <Th>Rows</Th>
                <Th>Access</Th>
                <Th>Last Analyzed</Th>
              </tr>
            </thead>
            <tbody>
              {data.map((item) => (
                <tr key={item.id}>
                  <Td>{item.schema_name}.{item.table_name}</Td>
                  <Td>{item.inferred_source_engine}</Td>
                  <Td>
                    <Badge type={item.data_category}>
                      {item.data_category}
                    </Badge>
                  </Td>
                  <Td>{item.business_domain}</Td>
                  <Td>
                    <Badge type={item.health_status}>
                      {item.health_status}
                    </Badge>
                  </Td>
                  <Td>
                    <Badge type={`${item.sensitivity_level}_SENSITIVITY`}>
                      {item.sensitivity_level}
                    </Badge>
                  </Td>
                  <Td>
                    <QualityScore score={item.data_quality_score}>
                      {item.data_quality_score}%
                    </QualityScore>
                  </Td>
                  <Td>{formatSize(item.table_size_mb)}</Td>
                  <Td>{formatNumber(item.total_rows)}</Td>
                  <Td>
                    <Badge type={item.access_frequency}>
                      {item.access_frequency}
                    </Badge>
                  </Td>
                  <Td>{formatDate(item.last_analyzed)}</Td>
                </tr>
              ))}
            </tbody>
          </Table>

          <Pagination>
            <PageButton
              disabled={page === 1}
              onClick={() => setPage(page - 1)}
            >
              Previous
            </PageButton>
            
            {Array.from({ length: pagination.totalPages }, (_, i) => i + 1)
              .filter(p => Math.abs(p - page) <= 2 || p === 1 || p === pagination.totalPages)
              .map((p, i, arr) => (
                <React.Fragment key={p}>
                  {i > 0 && arr[i - 1] !== p - 1 && <span>...</span>}
                  <PageButton
                    $active={p === page}
                    onClick={() => setPage(p)}
                  >
                    {p}
                  </PageButton>
                </React.Fragment>
              ))
            }
            
            <PageButton
              disabled={page === pagination.totalPages}
              onClick={() => setPage(page + 1)}
            >
              Next
            </PageButton>
          </Pagination>
        </>
      )}
    </GovernanceContainer>
  );
};

export default Governance;
