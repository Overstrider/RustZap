import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  allowedDevOrigins: ["localhost", "127.0.0.1", "0.0.0.0", "192.168.0.41", "192.168.0.36"]
};

export default nextConfig;
