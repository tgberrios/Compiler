import React from 'react';
import styled from 'styled-components';
import type { CatalogEntry } from '../services/api';

const BlurOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  backdrop-filter: blur(5px);
  background: rgba(0, 0, 0, 0.3);
  z-index: 999;
`;

const ModalOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
`;

const ModalContent = styled.div`
  background: white;
  padding: 20px;
  border-radius: 8px;
  min-width: 400px;
  font-family: monospace;
`;

const ModalHeader = styled.div`
  border-bottom: 2px solid #333;
  padding-bottom: 10px;
  margin-bottom: 20px;
  font-size: 1.2em;
  font-weight: bold;
`;

const FormGroup = styled.div`
  margin-bottom: 15px;
`;

const Label = styled.label`
  display: block;
  margin-bottom: 5px;
  font-weight: bold;
`;

const Select = styled.select`
  width: 100%;
  padding: 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-family: monospace;
`;

const Input = styled.input`
  width: 100%;
  padding: 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-family: monospace;
`;

const ButtonGroup = styled.div`
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 20px;
`;

const Button = styled.button`
  padding: 8px 16px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-family: monospace;

  &.primary {
    background: #1976d2;
    color: white;
  }

  &.secondary {
    background: #f5f5f5;
    color: #333;
  }
`;

interface EditModalProps {
  entry: CatalogEntry;
  onClose: () => void;
  onSave: (entry: CatalogEntry) => void;
}

const EditModal: React.FC<EditModalProps> = ({ entry, onClose, onSave }) => {
  const [editedEntry, setEditedEntry] = React.useState(entry);

  const handleSave = () => {
    onSave(editedEntry);
    onClose();
  };

  return (
    <>
      <BlurOverlay />
      <ModalOverlay>
      <ModalContent>
        <ModalHeader>Edit Table Configuration</ModalHeader>
        
        <FormGroup>
          <Label>Table</Label>
          <Input 
            type="text" 
            value={`${editedEntry.schema_name}.${editedEntry.table_name}`} 
            disabled 
          />
        </FormGroup>

        <FormGroup>
          <Label>Engine</Label>
          <Input 
            type="text" 
            value={editedEntry.db_engine} 
            disabled 
          />
        </FormGroup>

        <FormGroup>
          <Label>Status</Label>
          <Input
            type="text"
            value={editedEntry.status}
            onChange={(e) => setEditedEntry({...editedEntry, status: e.target.value})}
          />
        </FormGroup>

        <FormGroup>
          <Label>Active</Label>
          <Select
            value={editedEntry.active.toString()}
            onChange={(e) => setEditedEntry({...editedEntry, active: e.target.value === 'true'})}
          >
            <option value="true">Yes</option>
            <option value="false">No</option>
          </Select>
        </FormGroup>

        <FormGroup>
          <Label>Offset</Label>
          <Input
            type="text"
            value={editedEntry.last_offset}
            onChange={(e) => setEditedEntry({...editedEntry, last_offset: e.target.value})}
          />
        </FormGroup>

        <FormGroup>
          <Label>Cluster</Label>
          <Input
            type="text"
            value={editedEntry.cluster_name}
            onChange={(e) => setEditedEntry({...editedEntry, cluster_name: e.target.value})}
          />
        </FormGroup>

        <ButtonGroup>
          <Button className="secondary" onClick={onClose}>Cancel</Button>
          <Button className="primary" onClick={handleSave}>Save Changes</Button>
        </ButtonGroup>
      </ModalContent>
      </ModalOverlay>
    </>
  );
};

export default EditModal;
