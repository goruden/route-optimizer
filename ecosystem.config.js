module.exports = {
  apps: [
    {
      name: "route-optimizer-frontend",
      cwd: "/opt/route-optimizer/source/frontend",
      script: "pnpm",
      args: "start",
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: "production",
        PORT: 3067
      }
    }
  ],

  deploy: {
    dev: {
      user: "ubuntu",
      host: "54.191.84.231",
      ref: "origin/main",
      key: '~/.ssh/id_ed25519',
      repo: "https://github.com/goruden/route-optimizer.git",
      path: "/opt/route-optimizer",

      "pre-setup":
        "sudo mkdir -p /opt/route-optimizer && sudo chown -R ubuntu:ubuntu /opt/route-optimizer",

     "post-deploy":
  "cd /opt/route-optimizer/source/frontend && pnpm install && pnpm build && pm2 reload ecosystem.config.js --env dev"
    }
  }
};