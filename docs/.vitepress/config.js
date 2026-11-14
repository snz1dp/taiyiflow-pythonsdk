import { defineConfig } from 'vitepress'
import { existsSync } from 'node:fs'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { withMermaid } from 'vitepress-plugin-mermaid'

const basePath = process.env.VITEPRESS_BASE_PATH || '/taiyi/pythonsdk/docs/'
const __dirname = dirname(fileURLToPath(import.meta.url))
const hasGit = existsSync(resolve(__dirname, '../../.git'))

const config = defineConfig({
  title: '太乙智启 Python 开发手册',
  description: '太乙智启 Python SDK 使用手册：异步智能体、多会话、工具扩展、数字员工与部署',
  lang: 'zh-CN',
  base: basePath,
  outDir: '.vitepress/dist',
  lastUpdated: hasGit,
  ignoreDeadLinks: [
    /^https?:\/\/localhost/,
    /^https?:\/\/127\.0\.0\.1/
  ],
  themeConfig: {
    siteTitle: '太乙智启 Python SDK',
    darkModeSwitchLabel: '外观',
    lightModeSwitchTitle: '切换到浅色主题',
    darkModeSwitchTitle: '切换到深色主题',
    sidebarMenuLabel: '菜单',
    returnToTopLabel: '返回顶部',
    skipToContentLabel: '跳到主要内容',
    nav: [
      { text: '首页', link: '/' },
      { text: '快速开始', link: '/getting-started' },
      { text: '核心能力', link: '/agent-and-session' },
      { text: '数字员工', link: '/digital-worker' },
      { text: '部署', link: '/deployment' }
    ],
    sidebar: [
      {
        text: '入门',
        items: [
          { text: '快速开始', link: '/getting-started' },
          { text: '智能体与会话', link: '/agent-and-session' },
          { text: '示例程序', link: '/examples' }
        ]
      },
      {
        text: '核心能力',
        items: [
          { text: '钩子（Hook）体系与权限审批', link: '/hooks' },
          { text: '内置工具插件', link: '/plugins' },
          { text: '技能配置与供应', link: '/skills' },
          { text: 'MCP 接入', link: '/mcp' },
          { text: '执行控制与恢复', link: '/execution-control' },
          { text: '浏览器工具', link: '/browser' }
        ]
      },
      {
        text: '数字员工与协同',
        items: [
          { text: '数字员工', link: '/digital-worker' },
          { text: '协同插件', link: '/collaboration' }
        ]
      },
      {
        text: '集成与部署',
        items: [
          { text: '构建与部署', link: '/deployment' },
          { text: '质量验证与验收', link: '/verification' }
        ]
      },
      {
        text: '参考',
        items: [
          { text: '传输协议参考', link: '/protocol' }
        ]
      }
    ],
    search: {
      provider: 'local',
      options: {
        translations: {
          button: { buttonText: '搜索', buttonAriaLabel: '搜索文档' },
          modal: {
            displayDetails: '显示详情',
            resetButtonTitle: '清空',
            backButtonTitle: '关闭',
            noResultsText: '没有找到相关结果',
            footer: {
              selectText: '选择',
              navigateText: '切换',
              closeText: '关闭'
            }
          }
        }
      }
    },
    outline: { level: [2, 3], label: '页面导航' },
    docFooter: { prev: '上一篇', next: '下一篇' },
    lastUpdated: hasGit ? { text: '最后更新于' } : false,
    footer: {
      message: '太乙智启 Python SDK',
      copyright: '© 2026 长沙慧码至一信息科技有限公司'
    }
  },
  mermaid: { theme: 'default' },
  vite: {
    optimizeDeps: {
      include: [
        'fastdom',
        'fastdom/extensions/fastdom-promised.js',
        '@braintree/sanitize-url',
        'dayjs',
        'cytoscape',
        'cytoscape-cose-bilkent'
      ]
    }
  }
})

export default withMermaid(config)
