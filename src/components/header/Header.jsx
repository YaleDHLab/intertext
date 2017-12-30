import React from 'react';
import Navicon from './Navicon';
import Typeahead from '../typeahead/Typeahead';
import { Link } from 'react-router-dom';
import brand from '../../assets/images/intertext.png';

const Header = (props) => (
  <header>
    <div className='header-text'>
      <Link className='brand' to='/#'>
        <img src={brand} />
      </Link>
      <Typeahead />
      <Navicon />
    </div>
  </header>
)

export default Header;