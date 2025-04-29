import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "⛴️ ferry",
  description: "Lightweight & scalable data ingestion framework",
  base: "/ferry/",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    
    nav: [
        
        {
          text: 'Guide',
          items: [
            { text: 'Getting Started', link: '/guides/getting-started' },
            { text: 'API', link: '/ferry' },
            { text: 'gRPC', link: '/gRPC/getting-started' },
            { text: 'CLI', link: '/cli/coming-soon' },
            { text: 'Sources', link: '/sources/postgres' },
            { text: 'Destinations', link: '/sources/' }
          ]
        }
    ],

    sidebar: [
      {
        text: 'Guides',
        items: [
          { text: 'Getting Started', link: '/guides/getting-started' },
          { text: 'Why Ferry', link: '/guides/why-ferry' },
          { text: 'Write Disposition', link: '/guides/write-disposition' ,
            items: [
              { text: 'Replace', link: '/guides/wd-replace' },
              { text: 'Append', link: '/guides/wd-append' },
              { text: 'Merge', link: '/guides/wd-merge',
               items: [
                { text: 'delete-insert', link: '/guides/merge-delete-insert' },
                { text: 'scd2', link: '/guides/merge-scd2' },
                { text: 'upsert', link: '/guides/merge-upsert' },
              ]
              },

            ]

          },
          { text: 'Incremental Loading', link: '/guides/incremental-loading' },
          { text: 'Observability', link: '/guides/observability' },
        ]
      },
      {
        text: 'API',
        items: [
          { text: '/ferry', 
            items: [
              { text: '- source to destination', link: '/apis/basic' },
              { text: '- incremental loading', link: '/apis/incremental-loading' },
              { text: '- write disposition', 
                items: [
                  { text: 'replace', link: '/apis/replace-write-disposition' },
                  { text: 'append', link: '/apis/append-write-disposition' },
                  { text: 'merge', 
                    items: [
                      { text: 'delete-insert', link: '/apis/merge-delete-insert' },
                      { text: 'scd2', link: '/apis/merge-scd2' },
                      { text: 'upsert', link: '/apis/merge-upsert' }, 
                    ]
                  },
                  
                ]
              }
            ]
          },
          { text: '/ferry/{id}/observe', link: '/apis/observe' },
        ]
      },
      { text: 'CLI', link: '/cli/coming-soon' },
      { text: 'gRPC', link: '/gRPC/getting-started' },
      {
        text: 'Sources',
        items: [
          { text: 'Azure Storage', link: '' },
          { text: 'Clickhouse', link: '' },
          { text: 'DuckDb', link: '/sources/duckdb' },
          { text: 'Google Cloud Storage', link: '' },
          { text: 'Local File', link: '' },
          { text: 'Mariadb', link: '/sources/mariadb' },
          { text: 'MongoDb', link: '/sources/mongodb' },
          { text: 'MySQL', link: '/sources/mysql' },
          { text: 'Postgres', link: '/sources/postgres' },
          { text: 'S3', link: '/sources/s3' },
          { text: 'Sqlite', link: '/sources/sqlite' },
          { text: 'Kafka', link: '/sources/kafka' },

        ]
      },
      {
        text: 'Destinations',
        items: [
          { text: 'Athena', link: '' },
          { text: 'Bigquery', link: '' },
          { text: 'Clickhouse', link: '' },
          { text: 'Duckdb', link: '' },
          { text: 'Motherduck', link: '' },
          { text: 'MySQL', link: '' },
          { text: 'Postgres', link: '/destinations/postgres' },
          { text: 'Redshift', link: '' },
          
          
          { text: 'Snowflake', link: '' },
          { text: 'Sqlite', link: '' },
          
          
          
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/smalldata-ai/ferry' }
    ],
    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2025'
    },
    

  }
})
