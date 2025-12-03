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
  Brush,
} from "recharts";
import { DateRange } from "../../contexts/DateFilterContext";

type Region = "North" | "South" | "West" | "Houston" | "All";

interface HistoricalForecastCardProps {
  dateRange: DateRange;
  selectedModel: string;
}

interface LoadComparison {
  hour_end: string;
  coast_actual?: number;
  coast_expected?: number;
  east_actual?: number;
  east_expected?: number;
  far_west_actual?: number;
  far_west_expected?: number;
  north_actual?: number;
  north_expected?: number;
  north_c_actual?: number;
  north_c_expected?: number;
  southern_actual?: number;
  southern_expected?: number;
  south_c_actual?: number;
  south_c_expected?: number;
  west_actual?: number;
  west_expected?: number;
  ercot_actual?: number;
  ercot_expected?: number;
}

interface ChartDataPoint {
  time: string;
  historical: number;
  forecast: number;
}

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

const regionOptions: Region[] = ["All", "North", "South", "West", "Houston"];

export function HistoricalForecastCard({ dateRange, selectedModel }: HistoricalForecastCardProps) {
  const [selectedRegion, setSelectedRegion] = useState<Region>("All");
  const [data, setData] = useState<Record<Region, ChartDataPoint[]>>({
    All: [],
    North: [],
    South: [],
    West: [],
    Houston: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dateLabels, setDateLabels] = useState({ start: "", end: "" });

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

        // Map frontend model names to API parameter values
        const modelParam = selectedModel === "Statistical Model" ? "statistical" :
                          selectedModel === "XGBoost" ? "xgb" : "statistical";
        params.append("model", modelParam);

        const response = await fetch(
          `${API_BASE_URL}/load/comparison?${params.toString()}`
        );

        if (!response.ok) {
          throw new Error(`API error: ${response.status}`);
        }

        const apiData: LoadComparison[] = await response.json();

        // Extract date range from actual data
        if (apiData.length > 0) {
          const firstDate = new Date(apiData[0].hour_end);
          const lastDate = new Date(apiData[apiData.length - 1].hour_end);
          setDateLabels({
            start: firstDate.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" }),
            end: lastDate.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" }),
          });
        }

        // Transform data for each region
        const regionData: Record<Region, ChartDataPoint[]> = {
          All: [],
          North: [],
          South: [],
          West: [],
          Houston: [],
        };

        apiData.forEach((item) => {
          const time = new Date(item.hour_end).toLocaleString("en-US", {
            month: "short",
            day: "numeric",
            hour: "2-digit",
          });

          // All (ERCOT total)
          if (item.ercot_actual !== undefined && item.ercot_expected !== undefined) {
            regionData.All.push({
              time,
              historical: item.ercot_actual || 0,
              forecast: item.ercot_expected || 0,
            });
          }

          // North (north + north_c)
          if (
            item.north_actual !== undefined &&
            item.north_expected !== undefined &&
            item.north_c_actual !== undefined &&
            item.north_c_expected !== undefined
          ) {
            regionData.North.push({
              time,
              historical: (item.north_actual || 0) + (item.north_c_actual || 0),
              forecast: (item.north_expected || 0) + (item.north_c_expected || 0),
            });
          }

          // South (southern + south_c)
          if (
            item.southern_actual !== undefined &&
            item.southern_expected !== undefined &&
            item.south_c_actual !== undefined &&
            item.south_c_expected !== undefined
          ) {
            regionData.South.push({
              time,
              historical: (item.southern_actual || 0) + (item.south_c_actual || 0),
              forecast: (item.southern_expected || 0) + (item.south_c_expected || 0),
            });
          }

          // West (west + far_west)
          if (
            item.west_actual !== undefined &&
            item.west_expected !== undefined &&
            item.far_west_actual !== undefined &&
            item.far_west_expected !== undefined
          ) {
            regionData.West.push({
              time,
              historical: (item.west_actual || 0) + (item.far_west_actual || 0),
              forecast: (item.west_expected || 0) + (item.far_west_expected || 0),
            });
          }

          // Houston (coast + east)
          if (
            item.coast_actual !== undefined &&
            item.coast_expected !== undefined &&
            item.east_actual !== undefined &&
            item.east_expected !== undefined
          ) {
            regionData.Houston.push({
              time,
              historical: (item.coast_actual || 0) + (item.east_actual || 0),
              forecast: (item.coast_expected || 0) + (item.east_expected || 0),
            });
          }
        });

        setData(regionData);
      } catch (err) {
        console.error("Error fetching load comparison data:", err);
        setError(err instanceof Error ? err.message : "Failed to load data");
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [dateRange, selectedModel]);

  const chartData = data[selectedRegion] || [];

  return (
    <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
      <div className="flex items-center justify-between mb-2">
        <span className="text-[#011F5B] font-semibold text-center flex-1">
          Historical vs Forecasted Load by Region
        </span>
        <div className="flex items-center gap-2">
          <label className="text-xs text-gray-600">Region:</label>
          <select
            className="border border-gray-300 rounded px-2 py-1 text-xs"
            value={selectedRegion}
            onChange={(e) =>
              setSelectedRegion(e.target.value as Region)
            }
          >
            {regionOptions.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </div>
      </div>
      <div className="flex-1">
        {loading ? (
          <div className="flex items-center justify-center h-full">
            <span className="text-gray-500 text-sm">Loading...</span>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-full">
            <span className="text-red-500 text-sm">{error}</span>
          </div>
        ) : chartData.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <span className="text-gray-500 text-sm">No comparison data available</span>
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={chartData}
              margin={{ top: 10, right: 20, left: 0, bottom: 20 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="time" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line
                type="monotone"
                dataKey="historical"
                stroke="#011F5B"
                name="Historical"
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="forecast"
                stroke="#990000"
                name="Forecast"
                dot={false}
              />
              <Brush
                dataKey="time"
                height={30}
                stroke="#011F5B"
                fill="#f0f4f8"
                travellerWidth={10}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
