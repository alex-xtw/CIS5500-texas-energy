"use client";

import { DateRange } from "../../contexts/DateFilterContext";

type ModelType = "Statistical Model" | "Prophet" | "XGBoost";

interface ForecastConfigCardProps {
  dateRange: DateRange;
  selectedModel: string;
  onModelChange: (model: string) => void;
}

export function ForecastConfigCard({ selectedModel, onModelChange }: ForecastConfigCardProps) {
  return (
    <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
      <span className="text-[#011F5B] font-semibold mb-4">
        Forecast Configuration
      </span>

      <div className="flex flex-col">
        <label className="mb-2 text-sm text-gray-700">Model Type</label>
        <select
          className="border border-gray-300 rounded px-3 py-2 text-sm"
          value={selectedModel}
          onChange={(e) => onModelChange(e.target.value)}
        >
          <option value="Statistical Model">Statistical Model</option>
          <option value="Prophet">Prophet</option>
          <option value="XGBoost">XGBoost</option>
        </select>
      </div>
    </div>
  );
}
