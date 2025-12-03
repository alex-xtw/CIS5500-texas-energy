"use client";

import { useEffect, useState } from "react";
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
import { DateRange } from "../../contexts/DateFilterContext";

type Region = "all" | "North" | "South" | "West" | "Houston";

interface DryVsRainyChartProps {
  selectedRegion: Region;
  dateRange: DateRange;
}

interface PrecipitationData {
  zone: string;
  rainy_day: boolean;
  avg_load_mw: number;
  num_days: number;
}

interface ChartDataPoint {
  region: string;
  dry: number;
  rainy: number;
}

// Map API zones to display regions
const zoneToRegionMap: Record<string, string> = {
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

export function DryVsRainyChart({ selectedRegion, dateRange }: DryVsRainyChartProps) {
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
          params.append("start_date", dateRange.startDate);
        }
        if (dateRange.endDate) {
          params.append("end_date", dateRange.endDate);
        }

        const response = await fetch(
          `${API_BASE_URL}/weather/precipitation?${params.toString()}`
        );

        if (!response.ok) {
          throw new Error(`API error: ${response.status}`);
        }

        const apiData: PrecipitationData[] = await response.json();

        // Group data by display region
        const regionData: Record<string, { dry: number; rainy: number; dryCount: number; rainyCount: number }> = {
          North: { dry: 0, rainy: 0, dryCount: 0, rainyCount: 0 },
          South: { dry: 0, rainy: 0, dryCount: 0, rainyCount: 0 },
          West: { dry: 0, rainy: 0, dryCount: 0, rainyCount: 0 },
          Houston: { dry: 0, rainy: 0, dryCount: 0, rainyCount: 0 },
        };

        // Aggregate data by region
        apiData.forEach((item) => {
          const displayRegion = zoneToRegionMap[item.zone];
          if (displayRegion) {
            if (item.rainy_day) {
              regionData[displayRegion].rainy += item.avg_load_mw;
              regionData[displayRegion].rainyCount += 1;
            } else {
              regionData[displayRegion].dry += item.avg_load_mw;
              regionData[displayRegion].dryCount += 1;
            }
          }
        });

        // Convert to chart format, averaging the values
        const chartData: ChartDataPoint[] = Object.entries(regionData).map(
          ([region, values]) => ({
            region,
            dry: values.dryCount > 0 ? values.dry / values.dryCount : 0,
            rainy: values.rainyCount > 0 ? values.rainy / values.rainyCount : 0,
          })
        );

        setData(chartData);
      } catch (err) {
        console.error("Error fetching precipitation data:", err);
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
