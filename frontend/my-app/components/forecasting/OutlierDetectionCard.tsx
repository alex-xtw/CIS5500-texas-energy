"use client";

import { useEffect, useState } from "react";
import {
  ScatterChart,
  Scatter,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";
import { DateRange } from "../../contexts/DateFilterContext";

type Region = "North" | "South" | "West" | "Houston" | "All";

interface OutlierDetectionCardProps {
  dateRange: DateRange;
}

interface LoadOutlier {
  hour_end: string;
  region: string;
  load_mw: number;
  mean: number;
  std_dev: number;
  z_score: number;
  outlier_type: "high" | "low";
}

interface ScatterDataPoint {
  time: string;
  residual: number;
  region: string;
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
  ercot: "All",
};

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

const regionOptions: Region[] = ["All", "North", "South", "West", "Houston"];

const STD_DEV_THRESHOLD = 3;

export function OutlierDetectionCard({ dateRange }: OutlierDetectionCardProps) {
  const [outlierRegion, setOutlierRegion] = useState<Region>("All");
  const [data, setData] = useState<ScatterDataPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [thresholds, setThresholds] = useState<{ upper: number; lower: number }>({
    upper: 0,
    lower: 0,
  });

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);

        const params = new URLSearchParams({
          std_dev_threshold: STD_DEV_THRESHOLD.toString(),
          limit: "1000",
        });

        if (dateRange.startDate) {
          params.append("start_date", `${dateRange.startDate}T00:00:00`);
        }
        if (dateRange.endDate) {
          params.append("end_date", `${dateRange.endDate}T23:59:59`);
        }

        const response = await fetch(
          `${API_BASE_URL}/load/outliers?${params.toString()}`
        );

        if (!response.ok) {
          throw new Error(`API error: ${response.status}`);
        }

        const result = await response.json();
        const apiData: LoadOutlier[] = result.data;

        // Calculate residual (deviation from mean) and map to display regions
        const scatterData: ScatterDataPoint[] = apiData.map((outlier) => {
          const displayRegion = regionToDisplayMap[outlier.region] || outlier.region;
          const residual = outlier.load_mw - outlier.mean;

          return {
            time: new Date(outlier.hour_end).toLocaleString("en-US", {
              month: "short",
              day: "numeric",
              hour: "2-digit",
            }),
            residual: residual,
            region: displayRegion,
          };
        });

        setData(scatterData);

        // Calculate threshold lines based on the data
        if (apiData.length > 0) {
          // Use the std_dev from the first data point as reference
          const stdDev = apiData[0].std_dev;
          setThresholds({
            upper: STD_DEV_THRESHOLD * stdDev,
            lower: -STD_DEV_THRESHOLD * stdDev,
          });
        }
      } catch (err) {
        console.error("Error fetching outlier data:", err);
        setError(err instanceof Error ? err.message : "Failed to load data");
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [dateRange]);

  const residualsFiltered =
    outlierRegion === "All"
      ? data
      : data.filter((r) => r.region === outlierRegion);

  const isOutlier = (residual: number) =>
    residual > thresholds.upper || residual < thresholds.lower;

  const outliers = residualsFiltered.filter((r) => isOutlier(r.residual));

  return (
    <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
      <div className="flex items-center justify-between mb-2">
        <span className="text-[#011F5B] font-semibold">
          Outlier Detection (±3 SD)
        </span>
        <div className="flex items-center gap-2">
          <label className="text-xs text-gray-600">Region:</label>
          <select
            className="border border-gray-300 rounded px-2 py-1 text-xs"
            value={outlierRegion}
            onChange={(e) =>
              setOutlierRegion(e.target.value as Region)
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

      <div className="flex-1 flex">
        {loading ? (
          <div className="flex items-center justify-center w-full">
            <span className="text-gray-500 text-sm">Loading...</span>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center w-full">
            <span className="text-red-500 text-sm">{error}</span>
          </div>
        ) : data.length === 0 ? (
          <div className="flex items-center justify-center w-full">
            <span className="text-gray-500 text-sm">No outliers detected</span>
          </div>
        ) : (
          <>
            {/* Left: residual scatter */}
            <div className="flex-1">
              <ResponsiveContainer width="100%" height="100%">
                <ScatterChart margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="time" type="category" name="Time" />
                  <YAxis dataKey="residual" name="Residual (MW)" />
                  <Tooltip />
                  {thresholds.upper > 0 && (
                    <>
                      <ReferenceLine
                        y={thresholds.upper}
                        stroke="#999"
                        strokeDasharray="4 4"
                      />
                      <ReferenceLine
                        y={thresholds.lower}
                        stroke="#999"
                        strokeDasharray="4 4"
                      />
                    </>
                  )}
                  <Scatter
                    name="Outliers"
                    data={residualsFiltered}
                    fill="#011F5B"
                  />
                </ScatterChart>
              </ResponsiveContainer>
            </div>

            {/* Right: outlier list */}
            <div className="w-40 ml-3 text-xs">
              <div className="font-semibold mb-1">Outliers</div>
              <div className="border border-gray-200 rounded p-2 h-full overflow-auto">
                {outliers.length === 0 && (
                  <div className="text-gray-500">None detected</div>
                )}
                {outliers.slice(0, 20).map((r, idx) => (
                  <div
                    key={idx}
                    className="flex items-center justify-between mb-1"
                  >
                    <span className="text-red-600 font-bold mr-1">X</span>
                    <span className="text-[10px]">
                      {r.time} ({r.residual.toFixed(0)})
                    </span>
                  </div>
                ))}
                {outliers.length > 20 && (
                  <div className="text-gray-500 text-center mt-2">
                    +{outliers.length - 20} more
                  </div>
                )}
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
