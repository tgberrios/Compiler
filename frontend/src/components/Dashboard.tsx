import { useState } from 'react';
import styled from 'styled-components';

const DashboardContainer = styled.div`
  background-color: white;
  color: #333;
  padding: 20px;
  font-family: monospace;
  min-height: 100vh;
  width: 100vw;
  box-sizing: border-box;
  margin: 0;
  overflow-x: hidden;
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

const Section = styled.div`
  margin-bottom: 30px;
  padding: 20px;
  border: 1px solid #eee;
  border-radius: 4px;
  background-color: #fafafa;
`;

const ProgressBar = styled.div<{ progress: number }>`
  width: 100%;
  height: 20px;
  background-color: #ddd;
  margin: 10px 0;
  
  &:after {
    content: '';
    display: block;
    width: ${props => props.progress}%;
    height: 100%;
    background-color: #333;
  }
`;

const Grid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 20px;
  margin-top: 15px;
`;

const SectionTitle = styled.h3`
  margin-bottom: 15px;
  font-size: 1.2em;
  color: #222;
  border-bottom: 2px solid #333;
  padding-bottom: 8px;
`;

const Value = styled.div`
  font-size: 1.1em;
  padding: 8px;
  background-color: #fff;
  border-radius: 3px;
  border: 1px solid #ddd;
`;

const Dashboard = () => {
  const [syncStatus] = useState({
    progress: 75,
    perfectMatch: 9,
    listeningChanges: 6,
    fullLoadActive: 0,
    fullLoadInactive: 0,
    noData: 3,
    errors: 0,
    currentProcess: 'dbo.test_performance (NO_DATA)'
  });

  const [systemResources] = useState({
    cpuUsage: '0.0',
    memoryUsed: '12.90',
    memoryTotal: '30.54',
    memoryPercentage: '42.2',
    rss: '12.90',
    virtual: '19.35'
  });

  const [dbHealth] = useState({
    activeConnections: '1/100',
    responseTime: '< 1ms',
    bufferHitRate: '0.0',
    cacheHitRate: '0.0',
    status: 'Healthy'
  });

  const [connectionPool] = useState({
    totalPools: 4,
    activeConnections: 0,
    idleConnections: 0,
    failedConnections: 0,
    lastCleanup: '0m ago'
  });

  return (
    <DashboardContainer>
      <Header>
        DataSync Real-Time Dashboard
      </Header>

      <Section>
        <SectionTitle>■ SYNCHRONIZATION STATUS</SectionTitle>
        <ProgressBar progress={syncStatus.progress} />
        <Grid>
          <Value>Perfect Match: {syncStatus.perfectMatch}</Value>
          <Value>Listening Changes: {syncStatus.listeningChanges}</Value>
          <Value>Full Load (Active): {syncStatus.fullLoadActive}</Value>
          <Value>Full Load (Inactive): {syncStatus.fullLoadInactive}</Value>
          <Value>No Data: {syncStatus.noData}</Value>
          <Value>Errors: {syncStatus.errors}</Value>
        </Grid>
        <Value style={{ marginTop: '20px' }}>► Currently Processing: {syncStatus.currentProcess}</Value>
      </Section>

      <Section>
        <SectionTitle>▲ TRANSFER PERFORMANCE BY ENGINE</SectionTitle>
        <Value>No active transfers</Value>
        <Value style={{ marginTop: '10px' }}>System ready for synchronization</Value>
      </Section>

      <Section>
        <SectionTitle>● SYSTEM RESOURCES</SectionTitle>
        <Grid>
          <Value>CPU Usage: {systemResources.cpuUsage}% (0 cores)</Value>
          <Value>Memory: {systemResources.memoryUsed} GB/{systemResources.memoryTotal} GB ({systemResources.memoryPercentage}%)</Value>
          <Value>RSS: {systemResources.rss} GB</Value>
          <Value>Virtual: {systemResources.virtual} GB</Value>
        </Grid>
      </Section>

      <Section>
        <SectionTitle>■ DATABASE HEALTH</SectionTitle>
        <Grid>
          <Value>Active Connections: {dbHealth.activeConnections}</Value>
          <Value>Response Time: {dbHealth.responseTime}</Value>
          <Value>Buffer Hit Rate: {dbHealth.bufferHitRate}%</Value>
          <Value>Cache Hit Rate: {dbHealth.cacheHitRate}%</Value>
          <Value>Status: ✓ {dbHealth.status}</Value>
        </Grid>
      </Section>

      <Section>
        <SectionTitle>■ CONNECTION POOLING</SectionTitle>
        <Grid>
          <Value>Total Pools: {connectionPool.totalPools}</Value>
          <Value>Active Connections: {connectionPool.activeConnections}</Value>
          <Value>Idle Connections: {connectionPool.idleConnections}</Value>
          <Value>Failed Connections: {connectionPool.failedConnections}</Value>
          <Value>Last Cleanup: {connectionPool.lastCleanup}</Value>
        </Grid>
      </Section>
    </DashboardContainer>
  );
};

export default Dashboard;
