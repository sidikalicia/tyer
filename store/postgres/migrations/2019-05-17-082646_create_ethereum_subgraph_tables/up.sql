create schema ethereum_mainnet;

create table ethereum_mainnet.status (
  name              varchar primary key,
  head_block_hash   bytea,
  head_block_number numeric,
  check ((head_block_hash is null) = (head_block_number is null))
);

create table ethereum_mainnet.accounts (
  id      bytea primary key not null,
  address bytea unique not null,
  balance numeric not null
);

create table ethereum_mainnet.contracts (
  id      bytea primary key not null,
  address bytea unique not null,
  balance numeric not null
);

create table ethereum_mainnet.blocks (
  id                bytea primary key not null,

  hash              bytea not null,
  number            numeric,
  timestamp         numeric not null,
  author            bytea not null references ethereum_mainnet.accounts(id),
  nonce             bytea not null,

  parent            bytea not null references ethereum_mainnet.blocks(id),
  uncles_hash       bytea not null,
  -- TODO: Account for `uncles`

  state_root        bytea not null,
  transactions_root bytea not null,
  receipts_root     bytea not null,

  gas_used          numeric not null,
  gas_limit         numeric not null,
  extra_data        bytea not null,
  total_difficulty  numeric not null,
  -- TODO: Account for `seal_fields`
  size              numeric not null,
  mix_hash          bytea
);

create table ethereum_mainnet.transactions (
  id                bytea primary key not null,

  hash              bytea unique not null,
  block             bytea not null references ethereum_mainnet.blocks(id),
  transaction_index numeric not null,
  nonce             numeric not null,

  from_             bytea not null,
  to_               bytea,
  value_            bytea not null,
  input_            bytea not null,

  gas_price         numeric not null,
  gas               numeric not null
);

create table ethereum_mainnet.transaction_receipts (
  id                  bytea primary key not null,

  transaction         bytea unique not null references ethereum_mainnet.transactions(id),
  contract            bytea not null references ethereum_mainnet.contracts(id),
  logs_bloom          bytea not null,

  cumulative_gas_used numeric not null,
  gas_used            numeric not null,
  status              int2 not null
);

create table ethereum_mainnet.log (
  id                    bytea primary key not null,

  block                 bytea not null references ethereum_mainnet.blocks(id),
  transaction           bytea not null references ethereum_mainnet.transactions(id),
  log_index             numeric,
  transaction_log_index numeric,
  contract              bytea not null references ethereum_mainnet.contracts(id),

  -- TODO: Account for `topics`

  data                  bytea not null,
  log_type              varchar,
  removed               boolean not null
);
