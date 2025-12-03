"use client";

import { useEffect, useState } from "react";
import { DateRange } from "../../contexts/DateFilterContext";

type Region = "North" | "South" | "West" | "Houston" | "All";

interface MetricsCardProps {
  dateRange: DateRange;
  selectedModel: string;
}

interface ForecastMetrics {
  region: string;
  n: number;
  mse: number;
  mae: number;
  mape_pct: number;
  r2: number;
}

interface DisplayMetrics {
  region: string;
  mape: number;
  r2: number;
  mse: number;
  mae: number;
}

// Map API regions to display regions
const regionToDisplayMap: Record<string, string> = {
  coast: "Houston",
  east: "Houston",
  far_west: "West",
  north: "North",
  north_c: "North",
  southern: "South",
  south_c: "South",
  west: "West",
};

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

const regionOptions: Region[] = ["All", "North", "South", "West", "Houston"];

export function MetricsCard({ dateRange, selectedModel }: MetricsCardProps) {
  const [metricsRegion, setMetricsRegion] = useState<Region>("All");
  const [data, setData] = useState<DisplayMetrics[]>([]);
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

        // Map frontend model names to API parameter values
        const modelParam = selectedModel === "Statistical Model" ? "statistical" :
                          selectedModel === "XGBoost" ? "xgb" : "statistical";
        params.append("model", modelParam);

        const response = await fetch(
          `${API_BASE_URL}/forecast/metrics?${params.toString()}`
        );

        if (!response.ok) {
          throw new Error(`API error: ${response.status}`);
        }

        const apiData: ForecastMetrics[] = await response.json();

        // Group metrics by display region and calculate weighted averages
        const regionGroups: Record<string, {
          totalN: number;
          weightedMse: number;
          weightedMae: number;
          weightedMape: number;
          weightedR2: number;
        }> = {};

        apiData.forEach((metric) => {
          const displayRegion = regionToDisplayMap[metric.region] || metric.region;

          if (!regionGroups[displayRegion]) {
            regionGroups[displayRegion] = {
              totalN: 0,
              weightedMse: 0,
              weightedMae: 0,
              weightedMape: 0,
              weightedR2: 0,
            };
          }

          const group = regionGroups[displayRegion];
          group.totalN += metric.n;
          group.weightedMse += metric.mse * metric.n;
          group.weightedMae += metric.mae * metric.n;
          group.weightedMape += metric.mape_pct * metric.n;
          group.weightedR2 += metric.r2 * metric.n;
        });

        // Convert to display format
        const displayData: DisplayMetrics[] = Object.entries(regionGroups).map(
          ([region, group]) => ({
            region,
            mape: group.totalN > 0 ? group.weightedMape / group.totalN : 0,
            r2: group.totalN > 0 ? group.weightedR2 / group.totalN : 0,
            mse: group.totalN > 0 ? group.weightedMse / group.totalN : 0,
            mae: group.totalN > 0 ? group.weightedMae / group.totalN : 0,
          })
        );

        setData(displayData);
      } catch (err) {
        console.error("Error fetching forecast metrics:", err);
        setError(err instanceof Error ? err.message : "Failed to load data");
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [dateRange, selectedModel]);

  const metricsData =
    metricsRegion === "All"
      ? data
      : data.filter((m) => m.region === metricsRegion);

  return (
    <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
      <div className="flex items-center justify-between mb-2">
        <span className="text-[#011F5B] font-semibold">
          Forecast Performance (MAPE, R², MSE, MAE)
        </span>
        <div className="flex items-center gap-2">
          <label className="text-xs text-gray-600">Region:</label>
          <select
            className="border border-gray-300 rounded px-2 py-1 text-xs"
            value={metricsRegion}
            onChange={(e) =>
              setMetricsRegion(e.target.value as Region)
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

      <div className="flex-1 overflow-auto">
        {loading ? (
          <div className="flex items-center justify-center h-full">
            <span className="text-gray-500 text-sm">Loading...</span>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center h-full">
            <span className="text-red-500 text-sm">{error}</span>
          </div>
        ) : metricsData.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <span className="text-gray-500 text-sm">No metrics available</span>
          </div>
        ) : (
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="bg-gray-50">
                <th className="border border-gray-200 px-2 py-1 text-left">
                  Region
                </th>
                <th className="border border-gray-200 px-2 py-1 text-right">
                  MAPE (%)
                </th>
                <th className="border border-gray-200 px-2 py-1 text-right">
                  R²
                </th>
                <th className="border border-gray-200 px-2 py-1 text-right">
                  MSE
                </th>
                <th className="border border-gray-200 px-2 py-1 text-right">
                  MAE
                </th>
              </tr>
            </thead>
            <tbody>
              {metricsData.map((m) => (
                <tr key={m.region}>
                  <td className="border border-gray-200 px-2 py-1">
                    {m.region}
                  </td>
                  <td className="border border-gray-200 px-2 py-1 text-right">
                    {m.mape.toFixed(1)}
                  </td>
                  <td className="border border-gray-200 px-2 py-1 text-right">
                    {m.r2.toFixed(2)}
                  </td>
                  <td className="border border-gray-200 px-2 py-1 text-right">
                    {m.mse.toLocaleString()}
                  </td>
                  <td className="border border-gray-200 px-2 py-1 text-right">
                    {m.mae.toFixed(0)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
