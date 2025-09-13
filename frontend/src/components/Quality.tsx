import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { qualityApi } from '../services/api';

const QualityContainer = styled.div`
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

const QualityList = styled.div`
  display: flex;
  flex-direction: column;
  gap: 8px;
`;

const QualityItem = styled.div`
  border: 1px solid #eee;
  border-radius: 4px;
  background-color: #fafafa;
  overflow: hidden;
  transition: all 0.2s ease;
  
  &:hover {
    border-color: #ddd;
    background-color: #f5f5f5;
  }
`;

const QualitySummary = styled.div`
  display: grid;
  grid-template-columns: 150px 150px 100px 100px 100px 1fr;
  align-items: center;
  padding: 10px 15px;
  cursor: pointer;
  gap: 10px;
  font-size: 0.9em;
`;

const QualityDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '800px' : '0'};
  opacity: ${props => props.$isOpen ? '1' : '0'};
  transition: all 0.3s ease;
  border-top: ${props => props.$isOpen ? '1px solid #eee' : 'none'};
  background-color: white;
  overflow: hidden;
`;

const MetricsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  padding: 15px;
  gap: 15px;
`;

const MetricCard = styled.div`
  background: white;
  border: 1px solid #eee;
  border-radius: 4px;
  padding: 12px;
`;

const MetricLabel = styled.div`
  color: #666;
  font-size: 0.85em;
  margin-bottom: 5px;
`;

const MetricValue = styled.div`
  font-size: 1.1em;
  font-weight: 500;
`;

const ValidationStatus = styled.span<{ $status: string }>`
  padding: 3px 8px;
  border-radius: 3px;
  font-size: 0.85em;
  font-weight: 500;
  background-color: ${props => {
    switch (props.$status) {
      case 'PASSED': return '#e8f5e9';
      case 'WARNING': return '#fff3e0';
      case 'FAILED': return '#ffebee';
      default: return '#f5f5f5';
    }
  }};
  color: ${props => {
    switch (props.status) {
      case 'PASSED': return '#2e7d32';
      case 'WARNING': return '#ef6c00';
      case 'FAILED': return '#c62828';
      default: return '#757575';
    }
  }};
`;

const QualityScore = styled.span<{ $score: number }>`
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  font-weight: 500;
  background-color: ${props => {
    if (props.$score >= 90) return '#e8f5e9';
    if (props.$score >= 70) return '#f1f8e9';
    if (props.$score >= 50) return '#fff3e0';
    return '#ffebee';
  }};
  color: ${props => {
    if (props.$score >= 90) return '#1b5e20';
    if (props.$score >= 70) return '#33691e';
    if (props.$score >= 50) return '#e65100';
    return '#333333';
  }};
`;

const ErrorDetails = styled.pre`
  margin: 15px;
  padding: 12px;
  background-color: #f8f8f8;
  border-radius: 4px;
  font-size: 0.9em;
  overflow-x: auto;
  border: 1px solid #eee;
  white-space: pre-wrap;
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
  
  &:focus {
    outline: none;
    border-color: #666;
  }
`;

const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  margin-top: 20px;
  padding: 15px;
  font-size: 0.9em;
`;

const PageButton = styled.button<{ $active?: boolean }>`
  padding: 5px 10px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background: ${props => props.$active ? '#333' : 'white'};
  color: ${props => props.$active ? 'white' : '#333'};
  cursor: pointer;
  
  &:hover {
    background: ${props => props.active ? '#333' : '#f5f5f5'};
  }
  
  &:disabled {
    cursor: not-allowed;
    opacity: 0.5;
  }
`;

const Quality = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [qualityData, setQualityData] = useState<any[]>([]);
  const [openItemId, setOpenItemId] = useState<number | null>(null);
  
  // Filtros y paginación
  const [filter, setFilter] = useState({
    engine: '',
    status: ''
  });
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 10
  });

  useEffect(() => {
    const fetchQualityData = async () => {
      try {
        setError(null);
        const response = await qualityApi.getQualityMetrics({
          page,
          limit: 10,
          engine: filter.engine,
          status: filter.status
        });
        setQualityData(response.data);
        setPagination(response.pagination);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading quality data');
      } finally {
        setLoading(false);
      }
    };

    fetchQualityData();
    // Actualizar cada 30 segundos
    const interval = setInterval(fetchQualityData, 30000);
    return () => clearInterval(interval);
  }, [page, filter]);

  const toggleItem = (id: number) => {
    setOpenItemId(openItemId === id ? null : id);
  };

  const formatNumber = (num: number) => num.toLocaleString();

  const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearch(e.target.value);
    setPage(1);
  };

  const handleStatusChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setStatus(e.target.value);
    setPage(1);
  };

  const handleScoreChange = (value: number, type: 'min' | 'max') => {
    if (type === 'min') {
      setMinScore(Math.min(value, maxScore));
    } else {
      setMaxScore(Math.max(value, minScore));
    }
    setPage(1);
  };

  return (
    <QualityContainer>
      <Header>
        Data Quality Monitor
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
          value={filter.status}
          onChange={(e) => setFilter({...filter, status: e.target.value})}
        >
          <option value="">All Status</option>
          <option value="PASSED">Passed</option>
          <option value="WARNING">Warning</option>
          <option value="FAILED">Failed</option>
        </Select>
      </FiltersContainer>

      {loading && (
        <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
          Loading quality metrics...
        </div>
      )}

      {error && (
        <div style={{ color: 'red', padding: '20px', textAlign: 'center' }}>
          {error}
        </div>
      )}

      {!loading && !error && (
        <>
          <QualityList>
            {qualityData.length === 0 ? (
              <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
                No quality metrics found
              </div>
            ) : (
              qualityData.map((item) => (
                <QualityItem key={item.id}>
                  <QualitySummary onClick={() => toggleItem(item.id)}>
                    <div>{item.schema_name}</div>
                    <div>{item.table_name}</div>
                    <div>{formatNumber(item.total_rows)}</div>
                    <ValidationStatus $status={item.validation_status}>
                      {item.validation_status}
                    </ValidationStatus>
                    <QualityScore $score={item.quality_score}>
                      {item.quality_score}%
                    </QualityScore>
                    <div style={{ textAlign: 'right', color: '#666', fontSize: '0.85em' }}>
                      {new Date(item.check_timestamp).toLocaleString()}
                    </div>
                  </QualitySummary>

                  <QualityDetails $isOpen={openItemId === item.id}>
                    <MetricsGrid>
                      <MetricCard>
                        <MetricLabel>Null Values</MetricLabel>
                        <MetricValue>{formatNumber(item.null_count)}</MetricValue>
                      </MetricCard>
                      <MetricCard>
                        <MetricLabel>Duplicates</MetricLabel>
                        <MetricValue>{formatNumber(item.duplicate_count)}</MetricValue>
                      </MetricCard>
                      <MetricCard>
                        <MetricLabel>Invalid Types</MetricLabel>
                        <MetricValue>{formatNumber(item.invalid_type_count)}</MetricValue>
                      </MetricCard>
                      <MetricCard>
                        <MetricLabel>Out of Range</MetricLabel>
                        <MetricValue>{formatNumber(item.out_of_range_count)}</MetricValue>
                      </MetricCard>
                      <MetricCard>
                        <MetricLabel>Referential Integrity</MetricLabel>
                        <MetricValue>{formatNumber(item.referential_integrity_errors)}</MetricValue>
                      </MetricCard>
                      <MetricCard>
                        <MetricLabel>Constraint Violations</MetricLabel>
                        <MetricValue>{formatNumber(item.constraint_violation_count)}</MetricValue>
                      </MetricCard>
                      <MetricCard>
                        <MetricLabel>Check Duration</MetricLabel>
                        <MetricValue>{(item.check_duration_ms / 1000).toFixed(2)}s</MetricValue>
                      </MetricCard>
                    </MetricsGrid>

                    {item.type_mismatch_details && (
                      <>
                        <MetricLabel style={{ margin: '0 15px' }}>Type Mismatch Details:</MetricLabel>
                        <ErrorDetails>
                          {JSON.stringify(item.type_mismatch_details, null, 2)}
                        </ErrorDetails>
                      </>
                    )}

                    {item.integrity_check_details && (
                      <>
                        <MetricLabel style={{ margin: '0 15px' }}>Integrity Check Details:</MetricLabel>
                        <ErrorDetails>
                          {JSON.stringify(item.integrity_check_details, null, 2)}
                        </ErrorDetails>
                      </>
                    )}

                    {item.error_details && (
                      <>
                        <MetricLabel style={{ margin: '0 15px' }}>Error Details:</MetricLabel>
                        <ErrorDetails>{item.error_details}</ErrorDetails>
                      </>
                    )}
                  </QualityDetails>
                </QualityItem>
              ))
            )}
          </QualityList>

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
    </QualityContainer>
  );
};

export default Quality;