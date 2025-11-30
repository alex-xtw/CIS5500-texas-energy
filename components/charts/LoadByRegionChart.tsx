"use client";

import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

type Region = "all" | "North" | "South" | "West" | "Houston";

interface LoadByRegionChartProps {
  selectedRegion: Region;
}

const data = [
  { hour: "00:00", North: 2200, South: 1800, West: 1900, Houston: 2100 },
  { hour: "06:00", North: 2600, South: 2100, West: 2000, Houston: 2500 },
  { hour: "12:00", North: 3200, South: 2800, West: 2600, Houston: 3100 },
  { hour: "18:00", North: 3400, South: 3000, West: 2800, Houston: 3300 },
  { hour: "23:00", North: 2800, South: 2400, West: 2200, Houston: 2700 },
];

const REGION_LINES = [
  { key: "North", color: "#011F5B" },
  { key: "South", color: "#990000" },
  { key: "West", color: "#8884d8" },
  { key: "Houston", color: "#82ca9d" },
] as const;

export function LoadByRegionChart({ selectedRegion }: LoadByRegionChartProps) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="hour" />
        <YAxis />
        <Tooltip />
        <Legend />
        {REGION_LINES.filter(
          (r) => selectedRegion === "all" || r.key === selectedRegion
        ).map((region) => (
          <Line
            key={region.key}
            type="monotone"
            dataKey={region.key}
            stroke={region.color}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}
