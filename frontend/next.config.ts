import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  basePath: '/route-optimizer',
  turbopack: {},   // silence Turbopack warning; Leaflet only loads client-side anyway
  // output: 'standalone',
};

export default nextConfig;