/*
Copyright (C) 2025 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/

import React, { useEffect, useState } from 'react';
import { Tag, Typography } from '@douyinfe/semi-ui';
import { FileText, Download, ExternalLink } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { API } from '../../../helpers';

const { Text, Link } = Typography;

const formatFileSize = (bytes) => {
  if (!bytes || bytes <= 0) return '-';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
};

// ArchiveFiles lazily loads the objects archived for one relay request and
// renders their names plus download links. Renders nothing when the request
// has no archived files so the detail panel stays clean.
const ArchiveFiles = ({ requestId }) => {
  const { t } = useTranslation();
  const [files, setFiles] = useState(null);

  useEffect(() => {
    let cancelled = false;
    if (!requestId) {
      setFiles([]);
      return;
    }
    API.get(`/api/log/files?request_ids=${encodeURIComponent(requestId)}`)
      .then((res) => {
        if (cancelled) return;
        const { success, data } = res.data;
        setFiles(success ? data?.[requestId] || [] : []);
      })
      .catch(() => {
        if (!cancelled) setFiles([]);
      });
    return () => {
      cancelled = true;
    };
  }, [requestId]);

  if (!requestId) {
    return null;
  }
  if (files === null) {
    return (
      <span style={{ color: 'var(--semi-color-text-2)' }}>{t('加载中...')}</span>
    );
  }
  if (files.length === 0) {
    return (
      <span style={{ color: 'var(--semi-color-text-2)' }}>{t('（无文件）')}</span>
    );
  }

  return (
    <div className='flex flex-col gap-1.5' style={{ maxWidth: 600 }}>
      {files.map((file) => (
        <div key={file.id} className='flex items-center gap-2 flex-wrap'>
          <FileText size={14} className='shrink-0 text-gray-400' />
          <Text
            style={{ maxWidth: 260 }}
            ellipsis={{ showTitle: true }}
            copyable={false}
          >
            {file.filename || file.id}
          </Text>
          <Tag size='small' color={file.direction === 'input' ? 'blue' : 'green'}>
            {file.direction === 'input' ? t('用户上传') : t('AI生成')}
          </Tag>
          <Text type='tertiary' size='small'>
            {formatFileSize(file.size)}
          </Text>
          {file.status !== 'uploaded' && file.status !== 'partial' && (
            <Tag size='small' color='red'>
              {file.status}
            </Tag>
          )}
          {file.download_url && (
            <Link href={file.download_url} target='_blank' icon={<Download size={12} />}>
              {t('下载')}
            </Link>
          )}
          {file.original_url && (
            <Link href={file.original_url} target='_blank' icon={<ExternalLink size={12} />}>
              {t('源文件')}
            </Link>
          )}
        </div>
      ))}
    </div>
  );
};

export default ArchiveFiles;
