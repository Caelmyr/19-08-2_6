-- 协作文档编辑服务数据库Schema
CREATE DATABASE IF NOT EXISTS coedit DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE coedit;

-- 文档表：存储文档当前快照和版本信息
DROP TABLE IF EXISTS share_links;
DROP TABLE IF EXISTS operations;
DROP TABLE IF EXISTS documents;

CREATE TABLE documents (
    id VARCHAR(36) PRIMARY KEY COMMENT '文档UUID',
    title VARCHAR(255) NOT NULL DEFAULT '未命名文档' COMMENT '文档标题',
    content_snapshot TEXT COMMENT '当前文档内容快照',
    current_version BIGINT NOT NULL DEFAULT 0 COMMENT '当前版本号',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_version (current_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文档表';

-- 操作日志表：append-only的操作序列，支持增量同步和版本回滚
CREATE TABLE operations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    doc_id VARCHAR(36) NOT NULL COMMENT '文档ID',
    version BIGINT NOT NULL COMMENT '操作后的版本号',
    client_id VARCHAR(64) NOT NULL COMMENT '客户端标识',
    op_type ENUM('insert', 'delete') NOT NULL COMMENT '操作类型',
    position INT NOT NULL COMMENT '操作位置',
    length INT NOT NULL DEFAULT 0 COMMENT '删除长度',
    content TEXT COMMENT '插入内容',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_doc_version (doc_id, version),
    INDEX idx_doc_client (doc_id, client_id),
    FOREIGN KEY (doc_id) REFERENCES documents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='操作日志表';

-- 只读分享链接表
CREATE TABLE share_links (
    token VARCHAR(64) PRIMARY KEY COMMENT '分享令牌(URL携带)',
    doc_id VARCHAR(36) NOT NULL COMMENT '文档ID',
    expires_at DATETIME NULL COMMENT '过期时间，NULL=永不过期',
    revoked TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否已撤销',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_doc (doc_id),
    FOREIGN KEY (doc_id) REFERENCES documents(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='只读分享链接表';
