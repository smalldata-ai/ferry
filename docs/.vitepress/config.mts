import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "⛴️ ferry",
  description: "Lightweight & scalable data ingestion framework",
  base: "/ferry/",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      
      { text: 'Docs', link: '/guides/getting-started' }
    ],

    sidebar: [
      {
        text: 'Guides',
        items: [
          { text: 'Getting Started', link: '/guides/getting-started' },
          { text: 'Why Ferry', link: '/guides/why-ferry' },
          { text: 'Write Disposition', link: '/guides/write-disposition' },
          { text: 'Merge Strategy', link: '/guides/merge-strategy' },
          { text: 'Incremental Loading', link: '/guides/incremental-loading' },
          { text: 'Observability', link: '/guides/observability' },
        ]
      },
      {
        text: 'API',
        items: [
          { text: 'Ingest', link: '/rest-api/ingest' },
          { text: 'Metrics', link: '/rest-api/metrics' },
          { text: 'Schema', link: '/rest-api/schema' },
        ]
      },
      {
        text: 'CLI',
        items: [
          { text: 'Ingest', link: '/rest-api/ingest' },
          { text: 'Metrics', link: '/rest-api/metrics' },
          { text: 'Schema', link: '/rest-api/schema' },
        ]
      },
      {
        text: 'gRPC',
        items: [
          { text: 'Ingest', link: '/rest-api/ingest' },
          { text: 'Metrics', link: '/rest-api/metrics' },
          { text: 'Schema', link: '/rest-api/schema' },
        ]
      },
      {
        text: 'Sources',
        items: [
          { text: 'Postgres', link: '/sources/postgres' },
          { text: 'MySQL', link: '/sources/postgres' },
          { text: 'MongoDb', link: '/sources/postgres' },
          { text: 'DuckDb', link: '/sources/duckdb' },
          { text: 'S3', link: '/sources/s3' },
          { text: 'Sqlite', link: '/sources/sqlite' },
          { text: 'Clickhouse', link: '' },
          { text: 'Mariadb', link: '' },
          { text: 'Azure Storage', link: '' },
          { text: 'Google Cloud Storage', link: '' },
          { text: 'Local File', link: '' }
        ]
      },
      {
        text: 'Destinations',
        items: [
          { text: 'Postgres', link: '/destinations/postgres' },
          { text: 'Clickhouse', link: '' },
          { text: 'Duckdb', link: '' },
          { text: 'Snowflake', link: '' },
          { text: 'Sqlite', link: '' },
          { text: 'MySQL', link: '' },
          { text: 'Athena', link: '' },
          { text: 'Bigquery', link: '' },
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/smalldata-ai/ferry' }
    ],
    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2025'
    }

  }
})
