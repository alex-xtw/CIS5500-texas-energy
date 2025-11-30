"use client";

import { useState } from "react";

type ModelType = "Prophet" | "ARIMA" | "SARIMA";

interface SavedConfig {
  id: number;
  modelType: ModelType;
  trainSplit: number;
  versionName: string;
}

export function ForecastConfigCard() {
  const [modelType, setModelType] = useState<ModelType>("Prophet");
  const [trainSplit, setTrainSplit] = useState<number>(80);
  const [versionName, setVersionName] = useState<string>("v1_baseline");
  const [savedConfigs, setSavedConfigs] = useState<SavedConfig[]>([
    {
      id: 1,
      modelType: "Prophet",
      trainSplit: 80,
      versionName: "v0_demo",
    },
  ]);

  const handleSaveConfig = () => {
    if (!versionName.trim()) return;

    const newConfig: SavedConfig = {
      id: Date.now(),
      modelType,
      trainSplit,
      versionName: versionName.trim(),
    };

    setSavedConfigs((prev) => [newConfig, ...prev]);
    setVersionName("");
  };

  return (
    <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
      <span className="text-[#011F5B] font-semibold mb-2">
        Forecast Configuration
      </span>

      {/* Form */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-xs mb-4">
        <div className="flex flex-col">
          <label className="mb-1 text-gray-700">Model Type</label>
          <select
            className="border border-gray-300 rounded px-2 py-1"
            value={modelType}
            onChange={(e) =>
              setModelType(e.target.value as ModelType)
            }
          >
            <option value="Prophet">Facebook Prophet</option>
            <option value="ARIMA">ARIMA</option>
            <option value="SARIMA">SARIMA</option>
          </select>
        </div>

        <div className="flex flex-col">
          <label className="mb-1 text-gray-700">Train Split (%)</label>
          <input
            type="number"
            min={50}
            max={95}
            className="border border-gray-300 rounded px-2 py-1"
            value={trainSplit}
            onChange={(e) =>
              setTrainSplit(Number(e.target.value) || 0)
            }
          />
        </div>

        <div className="flex flex-col sm:col-span-2">
          <label className="mb-1 text-gray-700">Version Name</label>
          <input
            type="text"
            className="border border-gray-300 rounded px-2 py-1"
            placeholder="e.g., v1_sarima_peakload"
            value={versionName}
            onChange={(e) =>
              setVersionName(e.target.value)
            }
          />
        </div>
      </div>

      <button
        type="button"
        onClick={handleSaveConfig}
        className="self-start mb-3 inline-flex items-center justify-center rounded-md bg-[#011F5B] px-3 py-1 text-xs font-semibold text-white hover:bg-[#022769] transition"
      >
        Save Configuration
      </button>

      {/* Saved configs */}
      <div className="flex-1 overflow-auto border border-gray-200 rounded p-2 text-xs">
        {savedConfigs.length === 0 && (
          <div className="text-gray-500">
            No configurations saved yet.
          </div>
        )}
        {savedConfigs.map((cfg) => (
          <div
            key={cfg.id}
            className="flex justify-between items-center mb-1"
          >
            <div>
              <div className="font-semibold">{cfg.versionName}</div>
              <div className="text-gray-600">
                {cfg.modelType} · Train {cfg.trainSplit}%
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
