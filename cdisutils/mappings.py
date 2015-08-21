class Mapping(object):

    PHSID_TREE = {
        'phs000178': [],
        'phs000218': ['phs000463', 'phs000464', 'phs000465', 'phs000515',
                      'phs000471', 'phs000466', 'phs000470', 'phs000467',
                      'phs000468', 'phs000469']
    }

    PHSID_TO_PROJECT = {
        'phs000178': {
            'TCGA-LAML', 'TCGA-ACC', 'TCGA-BLCA', 'TCGA-LGG', 'TCGA-BRCA', 'TCGA-CESC', 'TCGA-CHOL', 'TCGA-COAD', 'TCGA-ESCA',
            'TCGA-GBM', 'TCGA-HNSC', 'TCGA-KICH', 'TCGA-KIRC', 'TCGA-KIRP', 'TCGA-LIHC', 'TCGA-LUAD', 'TCGA-LUSC', 'TCGA-DLBC',
            'TCGA-MESO', 'TCGA-OV', 'TCGA-PAAD', 'TCGA-PCPG', 'TCGA-PRAD', 'TCGA-READ', 'TCGA-SARC', 'TCGA-SKCM', 'TCGA-STAD',
            'TCGA-TGCT', 'TCGA-THYM', 'TCGA-THCA', 'TCGA-UCS', 'TCGA-UCEC', 'TCGA-UVM', 'TCGA-MISC', 'TCGA-LCML', 'TCGA-FPPP',
            'TCGA-CNTL'},
        'phs000218': [
            'TARGET-ALL-P1', 'TARGET-ALL-P2', 'TARGET-AML', 'TARGET-AML-IF', 'TARGET-WT', 'TARGET-CCSK', 'TARGET-RT', 'TARGET-NBL', 'TARGET-OS',
            'TARGET-MDLS'],
        'phs000463': ['TARGET-ALL-P1'],
        'phs000464': ['TARGET-ALL-P2'],
        'phs000465': ['TARGET-AML'],
        'phs000515': ['TARGET-AML-IF'],
        'phs000471': ['TARGET-WT'],
        'phs000466': ['TARGET-CCSK'],
        'phs000470': ['TARGET-RT'],
        'phs000467': ['TARGET-NBL'],
        'phs000468': ['TARGET-OS'],
        'phs000469': ['TARGET-MDLS']
    }

    PROJECT_TO_PHSID = {
        'TARGET-MDLS': ['phs000218', 'phs000469'],
        'TARGET-ALL-P1': ['phs000218', 'phs000463'],
        'TARGET-ALL-P2': ['phs000218', 'phs000464'],
        'TARGET-AML': ['phs000218', 'phs000465'],
        'TARGET-AML-IF': ['phs000218', 'phs000515'],
        'TARGET-CCSK': ['phs000218', 'phs000466'],
        'TARGET-NBL': ['phs000218', 'phs000467'],
        'TARGET-OS': ['phs000218', 'phs000468'],
        'TARGET-RT': ['phs000218', 'phs000470'],
        'TARGET-WT': ['phs000218', 'phs000471'],
        'TCGA-ACC': ['phs000178'],
        'TCGA-BLCA': ['phs000178'],
        'TCGA-BRCA': ['phs000178'],
        'TCGA-CESC': ['phs000178'],
        'TCGA-CHOL': ['phs000178'],
        'TCGA-CNTL': ['phs000178'],
        'TCGA-COAD': ['phs000178'],
        'TCGA-DLBC': ['phs000178'],
        'TCGA-ESCA': ['phs000178'],
        'TCGA-FPPP': ['phs000178'],
        'TCGA-GBM': ['phs000178'],
        'TCGA-HNSC': ['phs000178'],
        'TCGA-KICH': ['phs000178'],
        'TCGA-KIRC': ['phs000178'],
        'TCGA-KIRP': ['phs000178'],
        'TCGA-LAML': ['phs000178'],
        'TCGA-LCML': ['phs000178'],
        'TCGA-LGG': ['phs000178'],
        'TCGA-LIHC': ['phs000178'],
        'TCGA-LUAD': ['phs000178'],
        'TCGA-LUSC': ['phs000178'],
        'TCGA-MESO': ['phs000178'],
        'TCGA-MISC': ['phs000178'],
        'TCGA-OV': ['phs000178'],
        'TCGA-PAAD': ['phs000178'],
        'TCGA-PCPG': ['phs000178'],
        'TCGA-PRAD': ['phs000178'],
        'TCGA-READ': ['phs000178'],
        'TCGA-SARC': ['phs000178'],
        'TCGA-SKCM': ['phs000178'],
        'TCGA-STAD': ['phs000178'],
        'TCGA-TGCT': ['phs000178'],
        'TCGA-THCA': ['phs000178'],
        'TCGA-THYM': ['phs000178'],
        'TCGA-UCEC': ['phs000178'],
        'TCGA-UCS': ['phs000178'],
        'TCGA-UVM': ['phs000178']
    }

    def get_projects(self, phsid):
        '''Get a list of projects a program/project phsid map to'''
        return self.PHSID_TO_PROJECT.get(phsid, [])

    def get_phsids(self, project):
        '''Get a list of phsids a project map to'''
        return self.PROJECT_TO_PHSID.get(project, [])

    def get_project_level_phsid(self, project):
        '''
        Get project level phsid for a project name,
        return None if the project doesn't exist 
        '''
        phsids = self.get_phsids(project)
        if len(phsids) > 1: 
            return phsids[1]

    def get_program_level_phsid(self, project):
        '''
        Get program level phsid for a project name,
        return None if the project doesn't exist 
        '''
        phsids = self.get_phsids(project)
        if len(phsids) > 0: 
            return self.get_phsids(project)[0]

    def get_project(self, phsid):
        '''
        Get project name for a project phsid,
        return None for program phsid
        '''
        if phsid in self.PROGRAM_PHSID:
            return None
        projects = self.get_projects(phsid)
        if len(projects):
            return projects[0]
        return None
