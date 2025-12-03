"use client";

import { useEffect, useState } from "react";
import {
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { DateRange } from "../../contexts/DateFilterContext";

type Zone = "all" | "North" | "South" | "West" | "Houston";

interface HeatwavesByZoneChartProps {
  selectedZone: Zone;
  dateRange: DateRange;
}

interface HeatwaveData {
  zone: string;
  streak_start: string;
  streak_end: string;
  streak_days: number;
  avg_peak_load_mw: number | null;
}

interface ChartDataPoint {
  zone: string;
  heatwaves: number;
}

// Map API zones to display zones
const zoneToDisplayMap: Record<string, string> = {
  north: "North",
  north_c: "North",
  southern: "South",
  south_c: "South",
  west: "West",
  far_west: "West",
  coast: "Houston",
  east: "Houston",
};

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export function HeatwavesByZoneChart({ selectedZone, dateRange }: HeatwavesByZoneChartProps) {
  const [data, setData] = useState<ChartDataPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);

        const params = new URLSearchParams({
          min_temp_f: "100",
          min_days: "3",
        });

        if (dateRange.startDate) {
          params.append("start_date", dateRange.startDate);
        }
        if (dateRange.endDate) {
          params.append("end_date", dateRange.endDate);
        }

        const response = await fetch(
          `${API_BASE_URL}/weather/heatwaves?${params.toString()}`
        );

        if (!response.ok) {
          throw new Error(`API error: ${response.status}`);
        }

        const apiData: HeatwaveData[] = await response.json();

        // Count heatwave streaks by display zone
        const zoneCount: Record<string, number> = {
          North: 0,
          South: 0,
          West: 0,
          Houston: 0,
        };

        apiData.forEach((heatwave) => {
          const displayZone = zoneToDisplayMap[heatwave.zone];
          if (displayZone) {
            zoneCount[displayZone]++;
          }
        });

        // Convert to chart format
        const chartData: ChartDataPoint[] = Object.entries(zoneCount).map(
          ([zone, count]) => ({
            zone,
            heatwaves: count,
          })
        );

        setData(chartData);
      } catch (err) {
        console.error("Error fetching heatwave data:", err);
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
        <span className="text-gray-500 text-sm">No heatwaves detected</span>
      </div>
    );
  }

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
