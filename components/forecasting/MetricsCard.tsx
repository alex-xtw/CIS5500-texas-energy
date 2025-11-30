"use client";

import { useState } from "react";

type Region = "North" | "South" | "West" | "Houston" | "All";

const metricsByRegion = [
  { region: "North", mape: 3.2, r2: 0.91, mse: 12000, mae: 75 },
  { region: "South", mape: 3.8, r2: 0.89, mse: 15000, mae: 82 },
  { region: "West", mape: 4.5, r2: 0.87, mse: 18000, mae: 95 },
  { region: "Houston", mape: 2.9, r2: 0.93, mse: 10000, mae: 68 },
];

const regionOptions: Region[] = ["All", "North", "South", "West", "Houston"];

export function MetricsCard() {
  const [metricsRegion, setMetricsRegion] = useState<Region>("All");

  const metricsData =
    metricsRegion === "All"
      ? metricsByRegion
      : metricsByRegion.filter((m) => m.region === metricsRegion);

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
      </div>
    </div>
  );
}
