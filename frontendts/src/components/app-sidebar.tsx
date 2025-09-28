import { AccountMenu, ScheduleCallButton } from '@mundi/ee';
import { BookOpen, House, PanelRightClose, PanelRightOpen } from 'lucide-react';
import { Suspense } from 'react';
import { Link } from 'react-router-dom';
import MDarkSvg from '@/assets/N-dark.svg';
import MLightSvg from '@/assets/N-light.svg';
import MundiDarkSvg from '@/assets/Nasa-dark.svg';
import MundiLightSvg from '@/assets/Nasa-light.svg';
import { Button } from '@/components/ui/button';
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from '@/components/ui/sidebar';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { useProjects } from '@/contexts/ProjectsContext';
import type { MapProject } from '@/lib/types';
import { formatRelativeTime } from '@/lib/utils';

export function AppSidebar() {
  const { state, toggleSidebar } = useSidebar();
  const { allProjects, allProjectsLoading } = useProjects();

  const recentProjects: MapProject[] = allProjects
    .sort(
      (a, b) => new Date(b.most_recent_version?.last_edited || '').getTime() - new Date(a.most_recent_version?.last_edited || '').getTime(),
    )
    .slice(0, 3);

  return (
    <Sidebar collapsible="icon" data-theme="light" className="border-none">
      <SidebarHeader className="flex flex-col items-center p-4">
        {state === 'collapsed' ? (
          <>
            <a href="https://www.spaceappschallenge.org/2025/find-a-team/rooted/" target="_blank" className="w-10 h-10">
              <img src={MLightSvg} alt="M" className="w-full h-full dark:hidden" />
              <img src={MDarkSvg} alt="M" className="w-full h-full hidden dark:block" />
            </a>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button variant="ghost" size="icon" onClick={toggleSidebar} className="w-8 h-8 mt-2 cursor-pointer">
                  <PanelRightOpen className="w-4 h-4 scale-x-[-1]" />
                </Button>
              </TooltipTrigger>
              <TooltipContent side="right">
                <p>Expand Sidebar</p>
              </TooltipContent>
            </Tooltip>
          </>
        ) : (
          <div className="flex items-center justify-between w-full">
            <a href="https://www.spaceappschallenge.org/2025/find-a-team/rooted/" target="_blank" className="h-12">
              <img src={MundiLightSvg} alt="Mundi" className="h-full dark:hidden" />
              <img src={MundiDarkSvg} alt="Mundi" className="h-full hidden dark:block" />
            </a>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button variant="ghost" size="icon" onClick={toggleSidebar} className="w-8 h-8 cursor-pointer">
                  <PanelRightClose className="w-4 h-4 scale-x-[-1]" />
                </Button>
              </TooltipTrigger>
              <TooltipContent side="right">
                <p>Collapse Sidebar</p>
              </TooltipContent>
            </Tooltip>
          </div>
        )}
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Projects</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton asChild tooltip="Home">
                  <Link to={`/`}>
                    <House className="w-4 h-4 mr-2" />
                    <span className="text-sm">Home</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
              {!allProjectsLoading && state === 'expanded' && (
                <>
                  {recentProjects.map((project) => (
                    <SidebarMenuItem key={project.id}>
                      <SidebarMenuButton asChild>
                        <Link to={`/project/${project.id}`} className="flex items-center justify-between w-full">
                          <span className="text-sm">{project.title || `Untitled Map`}</span>
                          <span className="text-xs text-muted-foreground ml-2">
                            {formatRelativeTime(project.most_recent_version?.last_edited)}
                          </span>
                        </Link>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  ))}
                </>
              )}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <Suspense fallback={null}>
          <AccountMenu />
        </Suspense>
        <SidebarGroup>
          <SidebarGroupLabel>About</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <SidebarMenuItem>
                <SidebarMenuButton asChild tooltip="Documentation">
                  <a href="https://github.com/jjestrada2/farmane/blob/main/README.md" target="_blank">
                    <BookOpen className="w-4 h-4 mr-2" />
                    <span className="text-sm">Documentation</span>
                  </a>
                </SidebarMenuButton>
              </SidebarMenuItem>
              <SidebarMenuItem>
                <Suspense fallback={null}>
                  <ScheduleCallButton />
                </Suspense>
              </SidebarMenuItem>
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
