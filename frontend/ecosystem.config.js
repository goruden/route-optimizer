module.exports = {
  apps: [
    {
      name: "route-optimizer-frontend",
      cwd: "/opt/route-optimizer/current/frontend",
      script: "pnpm",
      args: "start",
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: "production",
        PORT: 3000
      }
    }
  ],

  deploy: {
    dev: {
      user: "ubuntu",
      host: "54.191.84.231",
      ref: "origin/main",
      key: '~/.ssh/qcomm-dev',
      repo: "https://github.com/goruden/route-optimizer.git",
      path: "/opt/route-optimizer",
      "pre-setup": "",
      "post-deploy": "cd /opt/route-optimizer/source/frontend && pnpm install && pnpm build && pm2 reload ../../ecosystem.config.js --only route-optimizer-frontend"
    }
  }
};