import React from 'react';
import { colors } from './colors';

const Legend = () => {
  return (
    <div className='chart-legend'>
      <span>50%</span>
      <div className='swatches'>
        {colors.map((c) => <div key={c} className='swatch' style={{background: c}} />)}
      </div>
      <span>100%</span>
    </div>
  )
}

export default Legend;