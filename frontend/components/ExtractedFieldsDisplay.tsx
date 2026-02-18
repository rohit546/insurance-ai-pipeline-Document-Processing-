'use client';

interface FlaggedField {
  value: any;
  color?: 'green' | 'red' | 'black';
}

interface ExtractedField {
  [key: string]: string | number | boolean | null | undefined | FlaggedField;
}

interface ExtractedFieldsDisplayProps {
  data: ExtractedField | null;
  title: string;
  color: 'blue' | 'green';
  isFlagged?: boolean;  // Whether data includes color flags
}

export default function ExtractedFieldsDisplay({
  data,
  title,
  color,
  isFlagged = false,
}: ExtractedFieldsDisplayProps) {
  if (!data) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-6 text-center">
        <p className="text-gray-500">No data available</p>
      </div>
    );
  }

  const bgColor = color === 'blue' ? 'bg-blue-50' : 'bg-green-50';
  const borderColor = color === 'blue' ? 'border-blue-200' : 'border-green-200';
  const titleColor = color === 'blue' ? 'text-blue-700' : 'text-green-700';
  const titleBgColor = color === 'blue' ? 'bg-blue-100' : 'bg-green-100';

  const formatValue = (value: any): string => {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'boolean') return value ? 'Yes' : 'No';
    if (Array.isArray(value)) return value.join(', ');
    return String(value);
  };

  const getColorStyle = (flagColor?: string) => {
    switch (flagColor) {
      case 'green':
        return 'text-green-700 font-semibold';  // Matches certificate ✅
      case 'red':
        return 'text-red-700 font-semibold';    // Mismatch ❌
      case 'black':
        return 'text-gray-700 font-semibold';   // Not in certificate
      default:
        return 'text-gray-700 font-semibold';
    }
  };

  const getBackgroundStyle = (flagColor?: string) => {
    switch (flagColor) {
      case 'green':
        return 'bg-green-50';
      case 'red':
        return 'bg-red-50';
      case 'black':
        return 'bg-gray-50';
      default:
        return 'hover:bg-white/50';
    }
  };

  // Flatten nested objects into separate fields
  const flattenData = (obj: Record<string, any>, prefix = ''): Array<[string, FlaggedField | any]> => {
    const result: Array<[string, FlaggedField | any]> = [];
    
    for (const [key, fieldData] of Object.entries(obj)) {
      if (fieldData === null || fieldData === undefined) continue;
      
      const newKey = prefix ? `${prefix} > ${key}` : key;
      
      // Check if this is flagged data (has 'value' and 'color' properties)
      if (isFlagged && fieldData && typeof fieldData === 'object' && 'value' in fieldData && 'color' in fieldData) {
        // This is a flagged field
        const flaggedField = fieldData as FlaggedField;
        
        // Check if the VALUE itself is a nested object
        if (flaggedField.value && typeof flaggedField.value === 'object' && !Array.isArray(flaggedField.value)) {
          // Recursively flatten the nested object, but preserve the color from the parent
          const nestedEntries = flattenData(flaggedField.value, newKey);
          // Apply the parent's color to all nested fields
          for (const [nestedKey, nestedData] of nestedEntries) {
            if (typeof nestedData === 'object' && 'value' in nestedData) {
              // Preserve the nested field's color if it has one
              result.push([nestedKey, nestedData]);
            } else {
              // Otherwise use parent's color
              result.push([nestedKey, { value: nestedData, color: flaggedField.color }]);
            }
          }
        } else {
          // Simple flagged field, push as-is
          result.push([newKey, flaggedField]);
        }
      } else if (fieldData && typeof fieldData === 'object' && !Array.isArray(fieldData)) {
        // Recursively flatten nested objects (not flagged)
        result.push(...flattenData(fieldData, newKey));
      } else {
        // Regular field
        result.push([newKey, { value: fieldData, color: 'black' }]);
      }
    }
    
    return result;
  };

  const entries = flattenData(data);

  return (
    <div className={`border border-gray-200 rounded-lg overflow-hidden ${bgColor}`}>
      {/* Header */}
      <div className={`${titleBgColor} px-6 py-3 border-b border-gray-200`}>
        <h3 className={`${titleColor} font-bold text-lg`}>{title}</h3>
      </div>

      {/* Fields */}
      <div className="divide-y divide-gray-200">
        {entries.length === 0 ? (
          <div className="px-6 py-4 text-center text-gray-500">
            No fields extracted
          </div>
        ) : (
          entries.map(([key, fieldData], index) => {
            const isFlaggedField = isFlagged && fieldData && typeof fieldData === 'object' && 'value' in fieldData;
            const displayValue = isFlaggedField ? fieldData.value : fieldData;
            const flagColor = isFlaggedField ? fieldData.color : undefined;
            const bgStyle = isFlaggedField ? getBackgroundStyle(flagColor) : 'hover:bg-white/50';
            const textStyle = isFlaggedField ? getColorStyle(flagColor) : 'text-gray-600';

            return (
              <div
                key={index}
                className={`px-6 py-4 ${bgStyle} transition-colors flex justify-between items-start gap-4`}
              >
                <label className="font-medium text-gray-700 flex-shrink-0 min-w-40">
                  {key.replace(/_/g, ' ')}:
                </label>
                <span className={`${textStyle} text-right break-words flex-1`}>
                  {formatValue(displayValue)}
                </span>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

