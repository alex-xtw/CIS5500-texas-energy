"use client";

import { useEffect, useState } from "react";
import { DateRange } from "../../contexts/DateFilterContext";

interface ExtremeHeatLoadTableProps {
  dateRange: DateRange;
}

interface ExtremeHeatData {
  zone: string;
  median_peak_load_mw: number;
  num_extreme_heat_days: number;
  threshold_percentile: number;
  threshold_temp_f: number;
}

interface RegionData {
  region: string;
  median_peak_load_mw: number;
  num_extreme_heat_days: number;
  avg_threshold_temp_f: number;
}

// Map API zones to display regions (same as LoadByRegionChart)
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

export function ExtremeHeatLoadTable({ dateRange }: ExtremeHeatLoadTableProps) {
  const [data, setData] = useState<RegionData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError(null);

      try {
        const params = new URLSearchParams();

        if (dateRange.startDate) {
          params.append("start_date", dateRange.startDate);
        }
        if (dateRange.endDate) {
          params.append("end_date", dateRange.endDate);
        }

        const url = `http://localhost:8000/load/peak-load-extreme-heat?${params.toString()}`;
        const response = await fetch(url);

        if (!response.ok) {
          throw new Error(`Failed to fetch data: ${response.statusText}`);
        }

        const apiData: ExtremeHeatData[] = await response.json();

        // Aggregate zones into regions
        const regionMap = new Map<string, { loads: number[], days: number[], temps: number[] }>();

        apiData.forEach((item) => {
          const region = zoneToRegionMap[item.zone];
          if (region) {
            if (!regionMap.has(region)) {
              regionMap.set(region, { loads: [], days: [], temps: [] });
            }
            const regionData = regionMap.get(region)!;
            regionData.loads.push(item.median_peak_load_mw);
            regionData.days.push(item.num_extreme_heat_days);
            regionData.temps.push(item.threshold_temp_f);
          }
        });

        // Convert to array and calculate aggregates
        const regionData: RegionData[] = Array.from(regionMap.entries()).map(([region, data]) => ({
          region,
          median_peak_load_mw: data.loads.reduce((a, b) => a + b, 0), // Sum the loads
          num_extreme_heat_days: Math.round(data.days.reduce((a, b) => a + b, 0) / data.days.length), // Average days
          avg_threshold_temp_f: data.temps.reduce((a, b) => a + b, 0) / data.temps.length, // Average temp
        }));

        // Sort by region name for consistent display
        regionData.sort((a, b) => a.region.localeCompare(b.region));

        setData(regionData);
      } catch (err) {
        setError(err instanceof Error ? err.message : "An error occurred");
        console.error("Error fetching extreme heat load data:", err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [dateRange]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-gray-500">Loading...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-red-500 text-sm">Error: {error}</p>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-gray-500 text-sm">No data available</p>
      </div>
    );
  }

  return (
    <div className="overflow-auto h-full">
      <table className="w-full text-sm">
        <thead className="bg-gray-100 sticky top-0">
          <tr>
            <th className="px-3 py-2 text-left font-semibold text-gray-700">Region</th>
            <th className="px-3 py-2 text-right font-semibold text-gray-700">Total Peak Load (MW)</th>
            <th className="px-3 py-2 text-right font-semibold text-gray-700">Avg Extreme Heat Days</th>
            <th className="px-3 py-2 text-right font-semibold text-gray-700">Avg Temp Threshold (°F)</th>
          </tr>
        </thead>
        <tbody>
          {data.map((row, index) => (
            <tr
              key={row.region}
              className={index % 2 === 0 ? "bg-white" : "bg-gray-50"}
            >
              <td className="px-3 py-2 text-left font-medium text-gray-900">{row.region}</td>
              <td className="px-3 py-2 text-right text-gray-700">
                {row.median_peak_load_mw.toFixed(2)}
              </td>
              <td className="px-3 py-2 text-right text-gray-700">
                {row.num_extreme_heat_days}
              </td>
              <td className="px-3 py-2 text-right text-gray-700">
                {row.avg_threshold_temp_f.toFixed(1)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
