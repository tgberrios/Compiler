import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { configApi } from '../services/api';
import type { ConfigEntry } from '../services/api';

const ConfigContainer = styled.div`
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

const ConfigTable = styled.table`
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
  font-family: 'Courier New', monospace;
`;

const Input = styled.input`
  width: 100%;
  padding: 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-family: 'Courier New', monospace;
  background: transparent;

  &:focus {
    outline: none;
    border-color: #666;
  }

  &:disabled {
    background: #f5f5f5;
    cursor: not-allowed;
  }
`;

const TextArea = styled.textarea`
  width: 100%;
  padding: 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-family: 'Courier New', monospace;
  resize: vertical;
  min-height: 60px;
  background: transparent;

  &:focus {
    outline: none;
    border-color: #666;
  }

  &:disabled {
    background: #f5f5f5;
    cursor: not-allowed;
  }
`;

const Button = styled.button<{ $variant?: 'primary' | 'danger' }>`
  padding: 8px 16px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-family: monospace;
  background-color: ${props => props.$variant === 'danger' ? '#ffebee' : '#e8f5e9'};
  color: ${props => props.$variant === 'danger' ? '#c62828' : '#2e7d32'};
  margin-right: 8px;

  &:hover {
    background-color: ${props => props.$variant === 'danger' ? '#ffcdd2' : '#c8e6c9'};
  }

  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
`;

const ActionCell = styled.td`
  padding: 12px;
  border-bottom: 1px solid #ddd;
  text-align: right;
`;

const AddButton = styled(Button)`
  margin: 20px 0;
`;

const Config = () => {
  const [configs, setConfigs] = useState<ConfigEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editForm, setEditForm] = useState<ConfigEntry | null>(null);

  const fetchConfigs = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await configApi.getConfigs();
      setConfigs(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error loading configurations');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchConfigs();
  }, []);

  const handleEdit = (config: ConfigEntry) => {
    setEditingKey(config.key);
    setEditForm({ ...config });
  };

  const handleCancel = () => {
    setEditingKey(null);
    setEditForm(null);
  };

  const handleSave = async () => {
    if (!editForm) return;

    try {
      await configApi.updateConfig(editForm);
      await fetchConfigs();
      setEditingKey(null);
      setEditForm(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error updating configuration');
    }
  };

  const handleAdd = () => {
    const newConfig: ConfigEntry = {
      key: '',
      value: '',
      description: '',
      updated_at: new Date().toISOString()
    };
    setEditingKey('new');
    setEditForm(newConfig);
  };

  const handleCreate = async () => {
    if (!editForm) return;

    try {
      await configApi.createConfig(editForm);
      await fetchConfigs();
      setEditingKey(null);
      setEditForm(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error creating configuration');
    }
  };

  return (
    <ConfigContainer>
      <Header>System Configuration</Header>

      {error && (
        <div style={{ color: 'red', padding: '20px', textAlign: 'center' }}>
          Error: {error}
        </div>
      )}

      <AddButton onClick={handleAdd}>+ Add New Configuration</AddButton>

      <ConfigTable>
        <thead>
          <tr>
            <Th>Key</Th>
            <Th>Value</Th>
            <Th>Description</Th>
            <Th>Last Updated</Th>
            <Th>Actions</Th>
          </tr>
        </thead>
        <tbody>
          {editingKey === 'new' && editForm && (
            <tr>
              <Td>
                <Input
                  value={editForm.key}
                  onChange={e => setEditForm({ ...editForm, key: e.target.value })}
                  placeholder="Enter key..."
                />
              </Td>
              <Td>
                <TextArea
                  value={editForm.value}
                  onChange={e => setEditForm({ ...editForm, value: e.target.value })}
                  placeholder="Enter value..."
                />
              </Td>
              <Td>
                <TextArea
                  value={editForm.description || ''}
                  onChange={e => setEditForm({ ...editForm, description: e.target.value })}
                  placeholder="Enter description..."
                />
              </Td>
              <Td>-</Td>
              <ActionCell>
                <Button onClick={handleCreate}>Save</Button>
                <Button $variant="danger" onClick={handleCancel}>Cancel</Button>
              </ActionCell>
            </tr>
          )}
          {configs.map(config => (
            <tr key={config.key}>
              <Td>
                {editingKey === config.key ? (
                  <Input
                    value={editForm?.key || ''}
                    onChange={e => setEditForm(prev => prev ? { ...prev, key: e.target.value } : null)}
                    disabled
                  />
                ) : (
                  config.key
                )}
              </Td>
              <Td>
                {editingKey === config.key ? (
                  <TextArea
                    value={editForm?.value || ''}
                    onChange={e => setEditForm(prev => prev ? { ...prev, value: e.target.value } : null)}
                  />
                ) : (
                  <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                    {config.value}
                  </pre>
                )}
              </Td>
              <Td>
                {editingKey === config.key ? (
                  <TextArea
                    value={editForm?.description || ''}
                    onChange={e => setEditForm(prev => prev ? { ...prev, description: e.target.value } : null)}
                  />
                ) : (
                  config.description || '-'
                )}
              </Td>
              <Td>{new Date(config.updated_at).toLocaleString()}</Td>
              <ActionCell>
                {editingKey === config.key ? (
                  <>
                    <Button onClick={handleSave}>Save</Button>
                    <Button $variant="danger" onClick={handleCancel}>Cancel</Button>
                  </>
                ) : (
                  <Button onClick={() => handleEdit(config)}>Edit</Button>
                )}
              </ActionCell>
            </tr>
          ))}
        </tbody>
      </ConfigTable>

      {loading && (
        <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
          Loading configurations...
        </div>
      )}
    </ConfigContainer>
  );
};

export default Config;
