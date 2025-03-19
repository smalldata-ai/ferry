import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "ferry",
  description: "A lightweight and scalable data ingestion framework",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      
      { text: 'Docs', link: '/markdown-examples' }
    ],

    sidebar: [
      {
        text: 'Examples',
        items: [
          { text: 'Markdown Examples', link: '/markdown-examples' },
          { text: 'Runtime API Examples', link: '/api-examples' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/smalldata-ai/' }
    ]
  }
})
