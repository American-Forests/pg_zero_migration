import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          root: './src',
          include: ['**/*.test.ts'],
          exclude: ['**/*.integration.test.ts'],
          environment: 'node',
        }
      },
      {
        test: {
          name: 'integration',
          root: './src',
          include: ['**/*.integration.test.ts'],
          environment: 'node'
        }
      }
    ]
  }
})