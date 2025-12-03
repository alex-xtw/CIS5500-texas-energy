"use client";

import { useEffect, useState } from "react";
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
import { DateRange } from "../../contexts/DateFilterContext";

type Region = "all" | "North" | "South" | "West" | "Houston";

interface LoadByRegionChartProps {
  selectedRegion: Region;
  dateRange: DateRange;
}

interface HourlyLoadData {
  hour_end: string;
  north: number;
  southern: number;
  south_c: number;
  west: number;
  north_c: number;
  coast: number;
  east: number;
  far_west: number;
}

interface ChartDataPoint {
  hour: string;
  North: number;
  South: number;
  West: number;
  Houston: number;
}

const REGION_LINES = [
  { key: "North", color: "#011F5B" },
  { key: "South", color: "#990000" },
  { key: "West", color: "#8884d8" },
  { key: "Houston", color: "#82ca9d" },
] as const;

// Map API regions to chart regions
const regionMapping = {
  North: ["north", "north_c"],
  South: ["southern", "south_c"],
  West: ["west", "far_west"],
  Houston: ["coast", "east"],
};

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
const MAX_CHART_POINTS = 100;

// Sample data evenly to reduce number of points
function sampleData<T>(data: T[], maxPoints: number): T[] {
  if (data.length <= maxPoints) {
    return data;
  }

  const step = data.length / maxPoints;
  const sampled: T[] = [];

  for (let i = 0; i < maxPoints; i++) {
    const index = Math.floor(i * step);
    sampled.push(data[index]);
  }

  return sampled;
}

export function LoadByRegionChart({ selectedRegion, dateRange }: LoadByRegionChartProps) {
  const [data, setData] = useState<ChartDataPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);

        const params = new URLSearchParams();

        if (dateRange.startDate) {
          params.append("start_date", `${dateRange.startDate}T00:00:00`);
        }
        if (dateRange.endDate) {
          params.append("end_date", `${dateRange.endDate}T23:59:59`);
        }

        const response = await fetch(
          `${API_BASE_URL}/load/hourly?${params.toString()}`
        );

        if (!response.ok) {
          throw new Error(`API error: ${response.status}`);
        }

        const apiData: HourlyLoadData[] = await response.json();

        // Transform API data to chart format
        const chartData: ChartDataPoint[] = apiData.map((item) => ({
          hour: new Date(item.hour_end).toLocaleString("en-US", {
            month: "short",
            day: "numeric",
            hour: "2-digit",
          }),
          North: (item.north || 0) + (item.north_c || 0),
          South: (item.southern || 0) + (item.south_c || 0),
          West: (item.west || 0) + (item.far_west || 0),
          Houston: (item.coast || 0) + (item.east || 0),
        }));

        // Sample data to maximum 100 points
        const sampledData = sampleData(chartData, MAX_CHART_POINTS);

        setData(sampledData);
      } catch (err) {
        console.error("Error fetching hourly load data:", err);
        setError(err instanceof Error ? err.message : "Failed to load data");
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [dateRange]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <span className="text-gray-500 text-sm">Loading...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <span className="text-red-500 text-sm">{error}</span>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center h-full">
        <span className="text-gray-500 text-sm">No data available</span>
      </div>
    );
  }

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
