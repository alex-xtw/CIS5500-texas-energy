"use client";

import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { DateRange } from "../../contexts/DateFilterContext";

interface ExtremeHeatLoadChartProps {
  dateRange: DateRange;
}

const data = [
  { day: "Day 1", load: 3400 },
  { day: "Day 2", load: 3600 },
  { day: "Day 3", load: 3550 },
  { day: "Day 4", load: 3700 },
  { day: "Day 5", load: 3650 },
];

export function ExtremeHeatLoadChart({ dateRange }: ExtremeHeatLoadChartProps) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="day" />
        <YAxis />
        <Tooltip />
        <Line type="monotone" dataKey="load" stroke="#990000" />
      </LineChart>
    </ResponsiveContainer>
  );
}
