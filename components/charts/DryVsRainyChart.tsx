"use client";

import {
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

type Region = "all" | "North" | "South" | "West" | "Houston";

interface DryVsRainyChartProps {
  selectedRegion: Region;
}

const data = [
  { region: "North", dry: 2800, rainy: 2500 },
  { region: "South", dry: 3000, rainy: 2700 },
  { region: "West", dry: 2600, rainy: 2400 },
  { region: "Houston", dry: 3100, rainy: 2900 },
];

export function DryVsRainyChart({ selectedRegion }: DryVsRainyChartProps) {
  const filteredData =
    selectedRegion === "all"
      ? data
      : data.filter((d) => d.region === selectedRegion);

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={filteredData} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="region" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Bar dataKey="dry" fill="#011F5B" name="Dry Days" />
        <Bar dataKey="rainy" fill="#8884d8" name="Rainy Days" />
      </BarChart>
    </ResponsiveContainer>
  );
}
