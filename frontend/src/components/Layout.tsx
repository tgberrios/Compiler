import { NavLink, Outlet } from 'react-router-dom';
import styled from 'styled-components';

const LayoutContainer = styled.div`
  display: flex;
  min-height: 100vh;
`;

const Sidebar = styled.div`
  width: 250px;
  background-color: #1a1a1a;
  color: white;
  padding: 20px 0;
  border-right: 1px solid #333;
`;

const MainContent = styled.div`
  flex: 1;
  background-color: white;
  overflow-y: auto;
`;

const NavItem = styled(NavLink)`
  display: flex;
  align-items: center;
  padding: 15px 25px;
  color: #888;
  text-decoration: none;
  font-family: monospace;
  font-size: 1.1em;
  border-left: 3px solid transparent;
  
  &:hover {
    background-color: #252525;
    color: white;
  }
  
  &.active {
    background-color: #252525;
    color: white;
    border-left-color: #4a9eff;
  }
`;

const Logo = styled.div`
  padding: 20px 25px;
  font-size: 1.3em;
  color: white;
  font-weight: bold;
  border-bottom: 1px solid #333;
  margin-bottom: 20px;
  font-family: monospace;
`;

const Layout = () => {
  return (
    <LayoutContainer>
      <Sidebar>
        <Logo>DataSync</Logo>
        <NavItem to="/" end>
          ■ Dashboard
        </NavItem>
        <NavItem to="/catalog">
          ▲ Catalog
        </NavItem>
        <NavItem to="/monitor">
          ● Monitor
        </NavItem>
        <NavItem to="/quality">
          ◆ Quality
        </NavItem>
              <NavItem to="/governance">
                ⬡ Governance
              </NavItem>
              <NavItem to="/config">
                ⚙ Config
              </NavItem>
      </Sidebar>
      <MainContent>
        <Outlet />
      </MainContent>
    </LayoutContainer>
  );
};

export default Layout;
