import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  turbopack: {},   // silence Turbopack warning; Leaflet only loads client-side anyway
  // output: 'standalone',
};

export default nextConfig;