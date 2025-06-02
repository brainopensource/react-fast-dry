import React, { useState, useEffect } from 'react';
import { Button, Progress, Alert, Space } from 'antd';
import { SyncOutlined } from '@ant-design/icons';
import axios from 'axios';

interface ImportStatus {
  id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  progress: number;
  total_records?: number;
  new_records?: number;
  duplicate_records?: number;
  error?: string;
}

const ImportButton: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);
  const [status, setStatus] = useState<ImportStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  const pollStatus = async (id: string) => {
    try {
      const response = await axios.get(`/api/v1/wells/import/status/${id}`);
      const newStatus = response.data.data;
      setStatus(newStatus);

      if (newStatus.status === 'running') {
        // Continue polling
        setTimeout(() => pollStatus(id), 1000);
      } else if (newStatus.status === 'failed') {
        setError(newStatus.error || 'Import failed');
        setLoading(false);
      } else if (newStatus.status === 'completed') {
        setLoading(false);
      }
    } catch (err) {
      setError('Failed to fetch import status');
      setLoading(false);
    }
  };

  const handleImport = async () => {
    try {
      setLoading(true);
      setError(null);
      setStatus(null);

      const response = await axios.get('/api/v1/wells/import/trigger');
      const newJobId = response.data.data.job_id;
      setJobId(newJobId);
      
      // Start polling for status
      pollStatus(newJobId);
    } catch (err: any) {
      if (err.response?.data?.message === 'An import is already in progress') {
        setError('An import is already in progress. Please wait for it to complete.');
      } else {
        setError('Failed to start import');
      }
      setLoading(false);
    }
  };

  const getStatusMessage = () => {
    if (!status) return null;

    switch (status.status) {
      case 'pending':
        return 'Import is pending...';
      case 'running':
        return `Importing data... ${status.progress}% complete`;
      case 'completed':
        return `Import completed: ${status.new_records} new records, ${status.duplicate_records} duplicates`;
      case 'failed':
        return `Import failed: ${status.error}`;
      default:
        return null;
    }
  };

  return (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Button
        type="primary"
        icon={<SyncOutlined spin={loading} />}
        onClick={handleImport}
        loading={loading}
        disabled={loading}
      >
        Import Data
      </Button>

      {status && (
        <Progress
          percent={status.progress}
          status={status.status === 'failed' ? 'exception' : 'active'}
          format={() => getStatusMessage()}
        />
      )}

      {error && (
        <Alert
          message="Error"
          description={error}
          type="error"
          showIcon
          closable
          onClose={() => setError(null)}
        />
      )}
    </Space>
  );
};

export default ImportButton; 