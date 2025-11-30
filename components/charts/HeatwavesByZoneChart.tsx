"use client";

import {
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

type Zone = "all" | "North" | "South" | "West" | "Houston";

interface HeatwavesByZoneChartProps {
  selectedZone: Zone;
}

const data = [
  { zone: "North", heatwaves: 5 },
  { zone: "South", heatwaves: 8 },
  { zone: "West", heatwaves: 4 },
  { zone: "Houston", heatwaves: 7 },
];

export function HeatwavesByZoneChart({ selectedZone }: HeatwavesByZoneChartProps) {
  const filteredData =
    selectedZone === "all"
      ? data
      : data.filter((d) => d.zone === selectedZone);

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={filteredData} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="zone" />
        <YAxis />
        <Tooltip />
        <Bar dataKey="heatwaves" fill="#011F5B" />
      </BarChart>
    </ResponsiveContainer>
  );
}
