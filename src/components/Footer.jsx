import React from 'react';
import mark from '../assets/images/yale-mark.svg';

class Footer extends React.Component {
  render() {
    return (
      <footer>
        <div className='footer-text'>
          <img className='yale-mark' src={mark} />
        </div>
        <div className='footer-text'>
          <div className='experiment'>An experiment of the Yale DHLab</div>
        </div>
      </footer>
    )
  }
}


export default Footer;